package requests

import (
	"fmt"
	"testing"

	"github.com/mansonxasthur/notifier/internal/domain"
)

// BenchmarkValidate measures validating one request of n recipients on all three channels.
// It is function speed, not capacity: no decoding, no database.
func BenchmarkValidate(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("recipients=%d", n), func(b *testing.B) {
			r := submission()
			for i := range n {
				r.Recipients = append(r.Recipients, rec(fmt.Sprintf("u_%05d", i), map[domain.Channel]string{
					domain.ChannelSMS:      fmt.Sprintf("+2010%08d", i),
					domain.ChannelEmail:    fmt.Sprintf("u%05d@example.com", i),
					domain.ChannelWhatsApp: fmt.Sprintf("+2011%08d", i),
				}))
			}
			p := Policy{BatchLimit: n, Expiry: policy.Expiry}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := r.Validate(p, now); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(n*3*b.N)/b.Elapsed().Seconds(), "deliveries/s")
		})
	}
}
