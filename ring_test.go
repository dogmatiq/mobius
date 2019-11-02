package mobius_test

import (

	// . "github.com/onsi/ginkgo/extensions/table"
	"fmt"
	"hash/crc32"
	"math"

	. "github.com/dogmatiq/mobius"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Ring", func() {
	var ring *Ring

	BeforeEach(func() {
		ring = &Ring{
			WeightMultiplier: 1,
		}
	})

	Describe("func Add()", func() {
		It("returns true if the member is added", func() {
			ok := ring.Add("<member-1>", []byte("<key>"), 1)
			Expect(ok).To(BeTrue())
		})

		It("returns false if the member is already in the ring", func() {
			ring.Add("<member-1>", []byte("<key>"), 1)
			ok := ring.Add("<member-1>", []byte("<key>"), 1)
			Expect(ok).To(BeFalse())
		})

		It("orders colliding members by weight", func() {
			ring.Add("<member-1>", []byte("<key>"), 1)
			ring.Add("<member-2>", []byte("<key>"), 2)

			m, ok := ring.Get([]byte("<key>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-2>"))
		})

		It("orders colliding members by weight regardless of the order they are added", func() {
			ring.Add("<member-2>", []byte("<key>"), 2)
			ring.Add("<member-1>", []byte("<key>"), 1)

			m, ok := ring.Get([]byte("<key>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-2>"))
		})

		It("orders colliding members by ID, if they have the same weight", func() {
			ring.Add("<member-2>", []byte("<key>"), 1)
			ring.Add("<member-1>", []byte("<key>"), 1)

			m, ok := ring.Get([]byte("<key>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-1>"))
		})

		It("uses a default weight multiplier", func() {
			ring.WeightMultiplier = 0
			ring.Add("<member-1>", []byte("<key>"), 1)

			m, ok := ring.Get([]byte("<key>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-1>"))
		})
	})

	Describe("func Remove()", func() {
		It("returns true if the member is removed", func() {
			ring.Add("<member-1>", []byte("<key>"), 1)
			ok := ring.Remove("<member-1>")
			Expect(ok).To(BeTrue())
		})

		It("returns false if the member is not already in the ring", func() {
			ok := ring.Remove("<member-1>")
			Expect(ok).To(BeFalse())
		})

		It("removes members from the ring", func() {
			ring.Add("<member-1>", []byte("<key>"), 1)
			ring.Add("<member-2>", []byte("<key>"), 2)
			ring.Remove("<member-2>")

			m, ok := ring.Get([]byte("<key>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-1>"))
		})
	})

	Describe("func Get()", func() {
		It("returns the associated member when a key matches exactly", func() {
			ring.Add("<member-1>", []byte("<key>"), 1)

			m, ok := ring.Get([]byte("<key>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-1>"))
		})

		It("returns the next member on the ring when the key does not match exactly", func() {
			// note that the IEEE CRC-32 hash used by default results in a member
			// ordering of: <key-2>, <key-3>, <key-1>. They are also chosen such
			// that they do not collide.
			ring.Add("<member-1>", []byte("<key-1>"), 1)
			ring.Add("<member-2>", []byte("<key-2>"), 1)
			ring.Add("<member-3>", []byte("<key-3>"), 1)

			m, ok := ring.Get(keyBefore("<key-2>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-2>"))

			m, ok = ring.Get(keyBetween("<key-2>", "<key-3>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-3>"))

			m, ok = ring.Get(keyBetween("<key-3>", "<key-1>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-1>"))

			m, ok = ring.Get(keyAfter("<key-1>"))
			Expect(ok).To(BeTrue())
			Expect(m).To(Equal("<member-2>"))
		})

		It("returns false if the ring is empty", func() {
			_, ok := ring.Get([]byte("<key>"))
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func Ordered()", func() {
		When("the ring is empty", func() {
			It("returns an empty slice", func() {
				members := ring.Ordered([]byte("<key>"))
				Expect(members).To(BeEmpty())
			})
		})

		When("the ring is not empty", func() {
			JustBeforeEach(func() {
				// note that the IEEE CRC-32 hash used by default results in a member
				// ordering of: <key-2>, <key-3>, <key-1>. They are also chosen such
				// that they do not collide.
				ring.Add("<member-1>", []byte("<key-1>"), 1)
				ring.Add("<member-2>", []byte("<key-2>"), 1)
				ring.Add("<member-3>", []byte("<key-3>"), 1)
			})

			It("returns the members in order of preference", func() {
				members := ring.Ordered(keyBefore("<key-2>"))
				Expect(members).To(Equal(
					[]string{
						"<member-2>",
						"<member-3>",
						"<member-1>",
					},
				))

				members = ring.Ordered(keyBetween("<key-2>", "<key-3>"))
				Expect(members).To(Equal(
					[]string{
						"<member-3>",
						"<member-1>",
						"<member-2>",
					},
				))

				members = ring.Ordered(keyBetween("<key-3>", "<key-1>"))
				Expect(members).To(Equal(
					[]string{
						"<member-1>",
						"<member-2>",
						"<member-3>",
					},
				))

				members = ring.Ordered(keyAfter("<key-1>"))
				Expect(members).To(Equal(
					[]string{
						"<member-2>",
						"<member-3>",
						"<member-1>",
					},
				))
			})

			When("the members appear in multiple nodes", func() {
				BeforeEach(func() {
					ring.WeightMultiplier = 2
				})

				It("returns the members in order of preference", func() {
					// crc32 ( <key-1>\x00 ) = 2755558954
					// crc32 ( <key-2>\x00 ) = 2792931443
					// crc32 ( <key-3>\x00 ) = 2814028356
					// crc32 ( <key-2> )     = 3422381187
					// crc32 ( <key-3> )     = 3538310594
					// crc32 ( <key-1> )     = 3771742016

					members := ring.Ordered(keyBefore("<key-1>\x00"))
					Expect(members).To(Equal(
						[]string{
							"<member-1>",
							"<member-2>",
							"<member-3>",
						},
					))

					members = ring.Ordered(keyBetween("<key-1>\x00", "<key-2>\x00"))
					Expect(members).To(Equal(
						[]string{
							"<member-2>",
							"<member-3>",
							"<member-1>",
						},
					))

					members = ring.Ordered(keyBetween("<key-2>\x00", "<key-3>\x00"))
					Expect(members).To(Equal(
						[]string{
							"<member-3>",
							"<member-2>",
							"<member-1>",
						},
					))

					members = ring.Ordered(keyBetween("<key-3>\x00", "<key-2>"))
					Expect(members).To(Equal(
						[]string{
							"<member-2>",
							"<member-3>",
							"<member-1>",
						},
					))

					members = ring.Ordered(keyBetween("<key-2>", "<key-3>"))
					Expect(members).To(Equal(
						[]string{
							"<member-3>",
							"<member-1>",
							"<member-2>",
						},
					))

					members = ring.Ordered(keyBetween("<key-3>", "<key-1>"))
					Expect(members).To(Equal(
						[]string{
							"<member-1>",
							"<member-2>",
							"<member-3>",
						},
					))

					members = ring.Ordered(keyAfter("<key-1>"))
					Expect(members).To(Equal(
						[]string{
							"<member-1>",
							"<member-2>",
							"<member-3>",
						},
					))
				})
			})
		})
	})
})

// keyBefore finds a key whose CRC32 hash is lower than the CRC32 hash of k.
func keyBefore(k string) []byte {
	hmax := crc32.ChecksumIEEE([]byte(k))

	for i := uint32(0); i < math.MaxUint32; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		h := crc32.ChecksumIEEE(key)

		if h < hmax {
			return key
		}
	}

	panic("key not found")
}

// keyAfter finds a key whose CRC32 hash is greater than the CRC32 hash of
// k.
func keyAfter(k string) []byte {
	hmin := crc32.ChecksumIEEE([]byte(k))

	for i := uint32(0); i < math.MaxUint32; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		h := crc32.ChecksumIEEE(key)

		if h > hmin {
			return key
		}
	}

	panic("key not found")
}

// keyBetween finds a key whose CRC32 hash falls between the CRC32 hash of
// min and max.
func keyBetween(min, max string) []byte {
	hmin := crc32.ChecksumIEEE([]byte(min))
	hmax := crc32.ChecksumIEEE([]byte(max))

	for i := uint32(0); i < math.MaxUint32; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		h := crc32.ChecksumIEEE(key)

		if h > hmin && h < hmax {
			return key
		}
	}

	panic("key not found")
}
