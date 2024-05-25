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
