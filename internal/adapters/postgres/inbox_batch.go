package postgres

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// The batch statements below are the single-row ones of inbox.go turned set-based: the same
// guards and fences, with the rows passed as parallel arrays.

const insertInboxMany = `
INSERT INTO inbox (delivery_id, state, provider, claim_count, channel)
SELECT d.id, $2, $3, 1, d.channel FROM deliveries d WHERE d.id = ANY($1) ORDER BY d.id
ON CONFLICT (delivery_id) DO NOTHING
RETURNING delivery_id`

const lockInboxMany = `
SELECT delivery_id, state, claim_count, deferrals, retries, provider, coalesce(next_attempt_at <= now(), true)
FROM inbox WHERE delivery_id = ANY($1) ORDER BY delivery_id FOR UPDATE`

const reclaimInboxMany = `
UPDATE inbox SET state = $2, claim_count = claim_count + 1, last_try = now()
WHERE delivery_id = ANY($1)`

const deadInboxMany = `
UPDATE inbox SET state = $2, failure_reason = $3, last_try = now()
WHERE delivery_id = ANY($1)`

const failDeliveryMany = `
UPDATE deliveries SET state = $2, failure_reason = $4, updated_at = now()
WHERE id = ANY($1) AND state = $3`

// ClaimMany implements ports.InboxStore.
func (s *InboxStore) ClaimMany(ctx context.Context, ids []int64, provider string, limits domain.ClaimLimits) ([]domain.InboxClaim, error) {
	claims := make([]domain.InboxClaim, len(ids))
	// Each id is claimed once, at its first place; the others report a duplicate, as a second Claim would.
	first := make(map[int64]int, len(ids))
	unique := make([]int64, 0, len(ids))
	for i, id := range ids {
		if _, seen := first[id]; seen {
			claims[i] = domain.InboxClaim{Result: domain.InboxClaimDuplicate}
			continue
		}
		first[id] = i
		unique = append(unique, id)
	}
	// A fixed lock order, so two senders claiming overlapping ids cannot deadlock.
	slices.Sort(unique)
	if len(unique) == 0 {
		return claims, nil
	}

	tx, err := s.db.Pool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim inbox batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, insertInboxMany, unique, domain.InboxProcessing, provider)
	if err != nil {
		return nil, fmt.Errorf("claim inbox batch: %w", err)
	}
	inserted, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("claim inbox batch: %w", err)
	}
	won := make(map[int64]bool, len(inserted))
	for _, id := range inserted {
		won[id] = true
		claims[first[id]] = domain.InboxClaim{Result: domain.InboxClaimWon, ClaimCount: 1, Provider: provider}
	}

	// Whatever did not insert either has a row already or has no delivery.
	var rest []int64
	for _, id := range unique {
		if !won[id] {
			rest = append(rest, id)
		}
	}
	if len(rest) > 0 {
		if err := claimExistingMany(ctx, tx, rest, first, claims, limits); err != nil {
			return nil, fmt.Errorf("claim inbox batch: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim inbox batch: %w", err)
	}
	return claims, nil
}

// claimExistingMany decides the ids that already have an inbox row, with the rules of claimExisting.
func claimExistingMany(ctx context.Context, tx pgx.Tx, ids []int64, first map[int64]int, claims []domain.InboxClaim, limits domain.ClaimLimits) error {
	type lockedRow struct {
		id                         int64
		state                      domain.InboxState
		claimCount, deferrals, ret int
		provider                   string
		due                        bool
	}
	rows, err := tx.Query(ctx, lockInboxMany, ids)
	if err != nil {
		return err
	}
	locked, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (lockedRow, error) {
		var l lockedRow
		err := r.Scan(&l.id, &l.state, &l.claimCount, &l.deferrals, &l.ret, &l.provider, &l.due)
		return l, err
	})
	if err != nil {
		return err
	}

	// An id that is in neither table has no delivery.
	found := make(map[int64]bool, len(locked))
	for _, l := range locked {
		found[l.id] = true
	}
	for _, id := range ids {
		if !found[id] {
			claims[first[id]] = domain.InboxClaim{Result: domain.InboxClaimMissing}
		}
	}

	var reclaim, dead []int64
	for _, l := range locked {
		i := first[l.id]
		switch {
		case l.state != domain.InboxWaitingRetry || !l.due:
			claims[i] = domain.InboxClaim{Result: domain.InboxClaimDuplicate}
		case limits.Used(l.claimCount, l.deferrals) >= limits.MaxClaims:
			claims[i] = domain.InboxClaim{Result: domain.InboxClaimDead}
			dead = append(dead, l.id)
		default:
			claims[i] = domain.InboxClaim{Result: domain.InboxClaimWon, ClaimCount: l.claimCount + 1, Retries: l.ret, Provider: l.provider}
			reclaim = append(reclaim, l.id)
		}
	}

	var b pgx.Batch
	if len(reclaim) > 0 {
		b.Queue(reclaimInboxMany, reclaim, domain.InboxProcessing)
	}
	if len(dead) > 0 {
		b.Queue(deadInboxMany, dead, domain.InboxDead, domain.ReasonMaxClaimsExceeded)
		b.Queue(failDeliveryMany, dead, domain.DeliveryFailed, domain.DeliveryPending, domain.ReasonMaxClaimsExceeded)
	}
	if b.Len() == 0 {
		return nil
	}
	br := tx.SendBatch(ctx, &b)
	for range b.Len() {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return err
		}
	}
	return br.Close()
}

// The settle statements: $1 holds the delivery ids and $2 the claim counts they must still carry.

const completeMany = `
WITH done AS (
  UPDATE inbox i SET state = $3, last_try = now()
  FROM unnest($1::bigint[], $2::int[]) AS o(id, claim)
  WHERE i.delivery_id = o.id AND i.state = $4 AND i.claim_count = o.claim
  RETURNING i.delivery_id
), accepted AS (
  UPDATE deliveries d SET state = $5, accepted_at = now(), updated_at = now()
  FROM done WHERE d.id = done.delivery_id AND d.state = $6
)
SELECT delivery_id FROM done`

const failMany = `
WITH dead AS (
  UPDATE inbox i SET state = $3, failure_class = NULLIF(o.class, ''), failure_reason = o.reason, last_try = now()
  FROM unnest($1::bigint[], $2::int[], $5::text[], $6::text[]) AS o(id, claim, class, reason)
  WHERE i.delivery_id = o.id AND i.state = $4 AND i.claim_count = o.claim
  RETURNING i.delivery_id, o.reason
), failed AS (
  UPDATE deliveries d SET state = $7, failure_reason = dead.reason, updated_at = now()
  FROM dead WHERE d.id = dead.delivery_id AND d.state = $8
)
SELECT delivery_id FROM dead`

const unresolveMany = `
WITH dead AS (
  UPDATE inbox i SET state = $3, failure_class = o.class, last_try = now()
  FROM unnest($1::bigint[], $2::int[], $5::text[]) AS o(id, claim, class)
  WHERE i.delivery_id = o.id AND i.state = $4 AND i.claim_count = o.claim
  RETURNING i.delivery_id
), unresolved AS (
  UPDATE deliveries d SET state = $6, updated_at = now()
  FROM dead WHERE d.id = dead.delivery_id AND d.state = $7
)
SELECT delivery_id FROM dead`

const parkMany = `
UPDATE inbox i SET state = $3, failure_class = NULLIF(o.class, ''), retries = i.retries + 1, next_attempt_at = o.next, last_try = now()
FROM unnest($1::bigint[], $2::int[], $5::text[], $6::timestamptz[]) AS o(id, claim, class, next)
WHERE i.delivery_id = o.id AND i.state = $4 AND i.claim_count = o.claim
RETURNING i.delivery_id`

const switchMany = `
UPDATE inbox i SET state = $3, provider = o.provider, retries = 0, failure_class = NULLIF(o.class, ''), next_attempt_at = o.next, last_try = now()
FROM unnest($1::bigint[], $2::int[], $5::text[], $6::text[], $7::timestamptz[]) AS o(id, claim, provider, class, next)
WHERE i.delivery_id = o.id AND i.state = $4 AND i.claim_count = o.claim
RETURNING i.delivery_id`

const deferMany = `
UPDATE inbox i SET state = $3, deferrals = i.deferrals + 1, next_attempt_at = o.next, last_try = now()
FROM unnest($1::bigint[], $2::int[], $5::timestamptz[]) AS o(id, claim, next)
WHERE i.delivery_id = o.id AND i.state = $4 AND i.claim_count = o.claim
RETURNING i.delivery_id`

// settleGroup is the outcomes of one kind, as the parallel arrays its statement takes, and the
// batch positions they came from.
type settleGroup struct {
	pos []int
	ids []int64
	cc  []int32
	// columns beyond the ids and claim counts, by kind.
	class, reason, provider []string
	next                    []time.Time
}

// Settle implements ports.InboxStore. One pipelined batch with one statement per kind present,
// so the whole call is one round trip and one commit.
func (s *InboxStore) Settle(ctx context.Context, outcomes []domain.InboxOutcome) ([]bool, error) {
	applied := make([]bool, len(outcomes))
	if len(outcomes) == 0 {
		return applied, nil
	}
	groups := map[domain.InboxOutcomeKind]*settleGroup{}
	var order []domain.InboxOutcomeKind
	for i, o := range outcomes {
		g := groups[o.Kind]
		if g == nil {
			g = &settleGroup{}
			groups[o.Kind] = g
			order = append(order, o.Kind)
		}
		g.pos = append(g.pos, i)
		g.ids = append(g.ids, o.DeliveryID)
		g.cc = append(g.cc, int32(o.ClaimCount)) //nolint:gosec // a claim count is a handful
		g.class = append(g.class, string(o.Class))
		g.reason = append(g.reason, string(o.Reason))
		g.provider = append(g.provider, o.Provider)
		g.next = append(g.next, o.Next)
	}

	var b pgx.Batch
	for _, kind := range order {
		g := groups[kind]
		switch kind {
		case domain.OutcomeComplete:
			b.Queue(completeMany, g.ids, g.cc, domain.InboxDone, domain.InboxProcessing, domain.DeliveryAccepted, domain.DeliveryPending)
		case domain.OutcomeFail:
			b.Queue(failMany, g.ids, g.cc, domain.InboxDead, domain.InboxProcessing, g.class, g.reason, domain.DeliveryFailed, domain.DeliveryPending)
		case domain.OutcomeUnresolve:
			b.Queue(unresolveMany, g.ids, g.cc, domain.InboxDead, domain.InboxProcessing, g.class, domain.DeliveryUnresolved, domain.DeliveryPending)
		case domain.OutcomePark:
			b.Queue(parkMany, g.ids, g.cc, domain.InboxWaitingRetry, domain.InboxProcessing, g.class, g.next)
		case domain.OutcomeSwitch:
			b.Queue(switchMany, g.ids, g.cc, domain.InboxWaitingRetry, domain.InboxProcessing, g.provider, g.class, g.next)
		case domain.OutcomeDefer:
			b.Queue(deferMany, g.ids, g.cc, domain.InboxWaitingRetry, domain.InboxProcessing, g.next)
		default:
			return nil, fmt.Errorf("settle inbox batch: unknown outcome kind %d", kind)
		}
	}

	br := s.db.Pool().SendBatch(ctx, &b)
	for _, kind := range order {
		g := groups[kind]
		rows, err := br.Query()
		if err != nil {
			_ = br.Close()
			return nil, fmt.Errorf("settle inbox batch: %w", err)
		}
		done, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			_ = br.Close()
			return nil, fmt.Errorf("settle inbox batch: %w", err)
		}
		// A row is applied when its delivery came back from its statement.
		returned := make(map[int64]bool, len(done))
		for _, id := range done {
			returned[id] = true
		}
		for j, p := range g.pos {
			applied[p] = returned[g.ids[j]]
		}
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("settle inbox batch: %w", err)
	}
	return applied, nil
}
