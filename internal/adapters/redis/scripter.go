package redis

import (
	"context"
	"errors"

	goredis "github.com/redis/go-redis/v9"
)

// errNotConnected is what a script call returns before Start has connected the client.
var errNotConnected = errors.New("redis: not connected")

// Scripter returns a script runner that uses the client once it has started. Callers can build
// their limiter before the lifecycle starts the client; until then every call fails, and the
// limiter's fallback answers instead of a nil client panicking.
func (c *Client) Scripter() goredis.Scripter { return lazyScripter{c} }

type lazyScripter struct{ c *Client }

func failed[T interface{ SetErr(error) }](cmd T) T {
	cmd.SetErr(errNotConnected)
	return cmd
}

func (s lazyScripter) Eval(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	client := s.c.client.Load()
	if client == nil {
		return failed(goredis.NewCmd(ctx))
	}
	return client.Eval(ctx, script, keys, args...)
}

func (s lazyScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *goredis.Cmd {
	client := s.c.client.Load()
	if client == nil {
		return failed(goredis.NewCmd(ctx))
	}
	return client.EvalSha(ctx, sha1, keys, args...)
}

func (s lazyScripter) EvalRO(ctx context.Context, script string, keys []string, args ...any) *goredis.Cmd {
	client := s.c.client.Load()
	if client == nil {
		return failed(goredis.NewCmd(ctx))
	}
	return client.EvalRO(ctx, script, keys, args...)
}

func (s lazyScripter) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) *goredis.Cmd {
	client := s.c.client.Load()
	if client == nil {
		return failed(goredis.NewCmd(ctx))
	}
	return client.EvalShaRO(ctx, sha1, keys, args...)
}

func (s lazyScripter) ScriptExists(ctx context.Context, hashes ...string) *goredis.BoolSliceCmd {
	client := s.c.client.Load()
	if client == nil {
		return failed(goredis.NewBoolSliceCmd(ctx))
	}
	return client.ScriptExists(ctx, hashes...)
}

func (s lazyScripter) ScriptLoad(ctx context.Context, script string) *goredis.StringCmd {
	client := s.c.client.Load()
	if client == nil {
		return failed(goredis.NewStringCmd(ctx))
	}
	return client.ScriptLoad(ctx, script)
}
