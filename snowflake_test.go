package snowflake

import (
	"fmt"
	"testing"
	"time"
)

func get(a int64) {
	fmt.Println(a)
	for i := 63; i >= 0; i-- {
		fmt.Print((a >> i) & 1)
	}
	fmt.Println()
}

func TestName(t *testing.T) {
	s, _ := NewSnowflake()
	get(s.NextID())
	get(s.NextID())
	get(s.NextID())
	time.Sleep(100 * time.Millisecond)
	get(s.NextID())
	get(s.NextID())
}

func TestNew(t *testing.T) {
	s, err := NewSnowflake()
	if err != nil {
		panic(err)
	}
	fmt.Println(s.NextID())
	fmt.Println(s.NextID())
	fmt.Println(s.NextID())
	time.Sleep(time.Second)
	fmt.Println(s.NextID())
	fmt.Println(s.NextID())
}

func TestParse(t *testing.T) {
	s, err := NewSnowflake()
	if err != nil {
		panic(err)
	}

	fmt.Println(Parse(uint64(s.NextID())))
	fmt.Println(Parse(uint64(s.NextID())))
	fmt.Println(Parse(uint64(s.NextID())))
	fmt.Println(Parse(uint64(s.NextID())))
	fmt.Println(Parse(uint64(s.NextID())))
	time.Sleep(time.Second)
	fmt.Println(Parse(uint64(s.NextID())))
	fmt.Println(Parse(uint64(s.NextID())))
}

func TestWithWorkID(t *testing.T) {
	var i int64 = 0

	s, err := NewSnowflake(WithWorkID(func() (int64, error) {
		i++
		return i, nil
	}))

	if err != nil {
		panic(err)
	}

	fmt.Println(Parse(uint64(s.NextID())))
	fmt.Println(Parse(uint64(s.NextID())))
	fmt.Println(Parse(uint64(s.NextID())))
}

func TestNonIncrement(t *testing.T) {
	s, err := NewSnowflake(WithNonIncrement())
	if err != nil {
		panic(err)
	}

	fmt.Println(s.NextID())
	fmt.Println(s.NextID())
	fmt.Println(s.NextID())

}
