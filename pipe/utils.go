package pipe

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"time"
	"unicode/utf8"

	log "github.com/sirupsen/logrus"
)

var privateIPBlocks []*net.IPNet

func init() {
	file, err := os.Open("./resources/ips-exclude.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cidr := scanner.Text()
		_, block, err := net.ParseCIDR(cidr)
		if err != nil {
			panic(fmt.Errorf("parse error on %q: %v", cidr, err))
		}
		privateIPBlocks = append(privateIPBlocks, block)
	}

	log.WithFields(log.Fields{"networks": len(privateIPBlocks)}).Info("blacklisted IPs loaded")

}

func isPrivateIp(ip net.IP) bool {

	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}

	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

// validHost checks if a hostname is resolvable and is not blacklisted
func IsValidHost(hostname string) bool {

	const timeout = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var r net.Resolver

	ips, err := r.LookupIP(ctx, "ip4", hostname)
	if err != nil {
		return true
	}

	if len(ips) <= 0 {
		return true
	}

	return !isPrivateIp(ips[0])
}

func ValidateDomain(name string) error {
	switch {
	case len(name) == 0:
		return nil // an empty domain name will result in a cookie without a domain restriction
	case len(name) > 255:
		return fmt.Errorf("cookie domain: name length is %d, can't exceed 255", len(name))
	}
	var l int
	for i := 0; i < len(name); i++ {
		b := name[i]
		if b == '.' {
			// check domain labels validity
			switch {
			case i == l:
				return fmt.Errorf("cookie domain: invalid character '%c' at offset %d: label can't begin with a period", b, i)
			case i-l > 63:
				return fmt.Errorf("cookie domain: byte length of label '%s' is %d, can't exceed 63", name[l:i], i-l)
			case name[l] == '-':
				return fmt.Errorf("cookie domain: label '%s' at offset %d begins with a hyphen", name[l:i], l)
			case name[i-1] == '-':
				return fmt.Errorf("cookie domain: label '%s' at offset %d ends with a hyphen", name[l:i], l)
			}
			l = i + 1
			continue
		}
		// test label character validity, note: tests are ordered by decreasing validity frequency
		if !(b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-' || b >= 'A' && b <= 'Z') {
			// show the printable unicode character starting at byte offset i
			c, _ := utf8.DecodeRuneInString(name[i:])
			if c == utf8.RuneError {
				return fmt.Errorf("cookie domain: invalid rune at offset %d", i)
			}
			return fmt.Errorf("cookie domain: invalid character '%c' at offset %d", c, i)
		}
	}
	// check top level domain validity
	switch {
	case l == len(name):
		return fmt.Errorf("cookie domain: missing top level domain, domain can't end with a period")
	case len(name)-l > 63:
		return fmt.Errorf("cookie domain: byte length of top level domain '%s' is %d, can't exceed 63", name[l:], len(name)-l)
	case name[l] == '-':
		return fmt.Errorf("cookie domain: top level domain '%s' at offset %d begins with a hyphen", name[l:], l)
	case name[len(name)-1] == '-':
		return fmt.Errorf("cookie domain: top level domain '%s' at offset %d ends with a hyphen", name[l:], l)
	case name[l] >= '0' && name[l] <= '9':
		return fmt.Errorf("cookie domain: top level domain '%s' at offset %d begins with a digit", name[l:], l)
	}
	return nil
}

// tables returns a list of tables names
// from a list of pipes
func Tables(pipes []Pipe) []string {
	tableMap := make(map[string]struct{})
	for _, p := range pipes {
		tableMap[p.Input.Table] = struct{}{}
		tableMap[p.Output.Table] = struct{}{}
	}

	var i []string
	for k := range tableMap {
		i = append(i, k)
	}

	return i
}
