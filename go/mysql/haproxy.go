package mysql

import (
	"fmt"
	"io"

	proto "github.com/pires/go-proxyproto"
)

// ReadHeaderfromHaproxyProto read header package from HaproxyProto
func (c *Conn) ReadHeaderfromHaproxyProto() (*proto.Header, error) {
	return proto.Read(c.bufferedReader)
}

// readProxyProtoPacket attempts to read a  mysql packet or ProtoPacket from the  byte buffer directly.
// It needs to be used for the first handshake packet the server receives,
// so we do't buffer the SSL negotiation packet. As a shortcut, only
// packets smaller than MaxPacketSize can be read here.
func (c *Conn) readProxyProtoPacket() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(fmt.Errorf("readEphemeralPacketDirect: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	var r io.Reader

	// c.reader.Buffered() > 0, this means that the header package has been
	// read in the reader.buffer or there is no header package.
	if c.bufferedReader.Buffered() > 0 {
		r = c.bufferedReader
	} else {
		// header package read, c.reader.Buffered() = 0
		r = c.conn
	}

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	if length < MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			return nil, fmt.Errorf("io.ReadFull(packet body of length %v) failed: %v", length, err)
		}
		return *c.currentEphemeralBuffer, nil
	}

	return nil, fmt.Errorf("readEphemeralPacketDirect doesn't support more than one packet")
}
