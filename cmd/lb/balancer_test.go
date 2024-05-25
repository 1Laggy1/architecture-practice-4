package main

import (
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestScheme(c *C) {
	*https = true
	c.Assert(scheme(), Equals, "https")

	*https = false
	c.Assert(scheme(), Equals, "http")
}

func (s *MySuite) TestHash(c *C) {
	str := "testString"
	c.Assert(hash(str), FitsTypeOf, uint32(0))
}

func (s *MySuite) TestLoadBalancer(c *C) {
	address1 := "192.168.110.10:54321"
	address2 := "192.168.110.20:54321"
	address3 := "172.151.110.40:54324"

	for i := range healthyServers {
		healthyServers[i] = true
	}

	firstServeraddress1 := chooseServer(address1)
	c.Assert(firstServeraddress1, Not(Equals), "")

	firstServeraddress2 := chooseServer(address2)
	c.Assert(firstServeraddress2, Not(Equals), "")

	firstServeraddress3 := chooseServer(address3)
	c.Assert(firstServeraddress3, Not(Equals), "")

	for i := 0; i < 10; i++ {
		serveraddress1 := chooseServer(address1)
		c.Assert(serveraddress1, Equals, firstServeraddress1)

		serveraddress2 := chooseServer(address2)
		c.Assert(serveraddress2, Equals, firstServeraddress2)

		serveraddress3 := chooseServer(address3)
		c.Assert(serveraddress3, Equals, firstServeraddress3)
	}
}

func TestHash(t *testing.T) {
	str := "testString"
	assert.IsType(t, uint32(0), hash(str), "Expected output type to be uint32")
}

func TestLoadBalancer(t *testing.T) {
	address1 := "192.168.110.10:54321"
	address2 := "192.168.110.20:54321"
	address3 := "172.151.110.40:54324"

	for i := range healthyServers {
		healthyServers[i] = true
	}

	firstServeraddress1 := chooseServer(address1)
	if firstServeraddress1 == "" {
		t.Fatal("No server chosen for address1")
	}

	firstServeraddress2 := chooseServer(address2)
	if firstServeraddress2 == "" {
		t.Fatal("No server chosen for address2")
	}

	firstServeraddress3 := chooseServer(address3)
	if firstServeraddress3 == "" {
		t.Fatal("No server chosen for address2")
	}

	for i := 0; i < 10; i++ {
		serveraddress1 := chooseServer(address1)
		if serveraddress1 != firstServeraddress1 {
			t.Fatalf("Different server chosen on iteration %d for address1. First server: %s, this iteration: %s", i, firstServeraddress1, serveraddress1)
		} else {
			fmt.Printf("Iteration %d | for address1 | Server %s was chosen | Hash: %d\n ", i, serveraddress1, hash(address1))
		}

		serveraddress2 := chooseServer(address2)
		if serveraddress2 != firstServeraddress2 {
			t.Fatalf("Different server chosen on iteration %d for address2. First server: %s, this iteration: %s", i, firstServeraddress2, serveraddress2)
		} else {
			fmt.Printf("Iteration %d | for address2 | Server %s was chosen | Hash: %d\n ", i, serveraddress2, hash(address2))
		}

		serveraddress3 := chooseServer(address3)
		if serveraddress3 != firstServeraddress3 {
			t.Fatalf("Different server chosen on iteration %d for address3. First server: %s, this iteration: %s", i, firstServeraddress3, serveraddress3)
		} else {
			fmt.Printf("Iteration %d | for address3 | Server %s was chosen | Hash: %d\n ", i, serveraddress3, hash(address3))
		}
	}
}