// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package httpsec

import (
	"math/rand"
	"net/http"
	"testing"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"

	"github.com/stretchr/testify/require"
)

func TestNormalizeHTTPHeaders(t *testing.T) {
	for _, tc := range []struct {
		headers  map[string][]string
		expected map[string]string
	}{
		{
			headers:  nil,
			expected: nil,
		},
		{
			headers: map[string][]string{
				"cookie": {"not-collected"},
			},
			expected: nil,
		},
		{
			headers: map[string][]string{
				"cookie":          {"not-collected"},
				"x-forwarded-for": {"1.2.3.4,5.6.7.8"},
			},
			expected: map[string]string{
				"x-forwarded-for": "1.2.3.4,5.6.7.8",
			},
		},
		{
			headers: map[string][]string{
				"cookie":          {"not-collected"},
				"x-forwarded-for": {"1.2.3.4,5.6.7.8", "9.10.11.12,13.14.15.16"},
			},
			expected: map[string]string{
				"x-forwarded-for": "1.2.3.4,5.6.7.8,9.10.11.12,13.14.15.16",
			},
		},
	} {
		headers := NormalizeHTTPHeaders(tc.headers)
		require.Equal(t, tc.expected, headers)
	}
}

type ipTestCase struct {
	name           string
	remoteAddr     string
	headers        map[string]string
	expectedIP     netaddrIP
	multiHeaders   string
	clientIPHeader string
}

func genIPTestCases() []ipTestCase {
	ipv4Global := randGlobalIPv4().String()
	ipv6Global := randGlobalIPv6().String()
	ipv4Private := randPrivateIPv4().String()
	ipv6Private := randPrivateIPv6().String()
	tcs := []ipTestCase{}
	// Simple ipv4 test cases over all headers
	for _, header := range defaultIPHeaders {
		tcs = append(tcs, ipTestCase{
			name:       "ipv4-global." + header,
			headers:    map[string]string{header: ipv4Global},
			expectedIP: netaddrMustParseIP(ipv4Global),
		})
		tcs = append(tcs, ipTestCase{
			name:       "ipv4-private." + header,
			headers:    map[string]string{header: ipv4Private},
			expectedIP: netaddrIP{},
		})
	}
	// Simple ipv6 test cases over all headers
	for _, header := range defaultIPHeaders {
		tcs = append(tcs, ipTestCase{
			name:       "ipv6-global." + header,
			headers:    map[string]string{header: ipv6Global},
			expectedIP: netaddrMustParseIP(ipv6Global),
		})
		tcs = append(tcs, ipTestCase{
			name:       "ipv6-private." + header,
			headers:    map[string]string{header: ipv6Private},
			expectedIP: netaddrIP{},
		})
	}
	// private and global in same header
	tcs = append([]ipTestCase{
		{
			name:       "ipv4-private+global",
			headers:    map[string]string{"x-forwarded-for": ipv4Private + "," + ipv4Global},
			expectedIP: netaddrMustParseIP(ipv4Global),
		},
		{
			name:       "ipv4-global+private",
			headers:    map[string]string{"x-forwarded-for": ipv4Global + "," + ipv4Private},
			expectedIP: netaddrMustParseIP(ipv4Global),
		},
		{
			name:       "ipv6-private+global",
			headers:    map[string]string{"x-forwarded-for": ipv6Private + "," + ipv6Global},
			expectedIP: netaddrMustParseIP(ipv6Global),
		},
		{
			name:       "ipv6-global+private",
			headers:    map[string]string{"x-forwarded-for": ipv6Global + "," + ipv6Private},
			expectedIP: netaddrMustParseIP(ipv6Global),
		},
	}, tcs...)
	// Invalid IPs (or a mix of valid/invalid over a single or multiple headers)
	tcs = append([]ipTestCase{
		{
			name:       "invalid-ipv4",
			headers:    map[string]string{"x-forwarded-for": "127..0.0.1"},
			expectedIP: netaddrIP{},
		},
		{
			name:       "invalid-ipv4-recover",
			headers:    map[string]string{"x-forwarded-for": "127..0.0.1, " + ipv4Global},
			expectedIP: netaddrMustParseIP(ipv4Global),
		},
		{
			name:         "ipv4-multi-header-1",
			headers:      map[string]string{"x-forwarded-for": "127.0.0.1", "forwarded-for": ipv4Global},
			expectedIP:   netaddrIP{},
			multiHeaders: "x-forwarded-for,forwarded-for",
		},
		{
			name:         "ipv4-multi-header-2",
			headers:      map[string]string{"forwarded-for": ipv4Global, "x-forwarded-for": "127.0.0.1"},
			expectedIP:   netaddrIP{},
			multiHeaders: "x-forwarded-for,forwarded-for",
		},
		{
			name:       "invalid-ipv6",
			headers:    map[string]string{"x-forwarded-for": "2001:0db8:2001:zzzz::"},
			expectedIP: netaddrIP{},
		},
		{
			name:       "invalid-ipv6-recover",
			headers:    map[string]string{"x-forwarded-for": "2001:0db8:2001:zzzz::, " + ipv6Global},
			expectedIP: netaddrMustParseIP(ipv6Global),
		},
		{
			name:         "ipv6-multi-header-1",
			headers:      map[string]string{"x-forwarded-for": "2001:0db8:2001:zzzz::", "forwarded-for": ipv6Global},
			expectedIP:   netaddrIP{},
			multiHeaders: "x-forwarded-for,forwarded-for",
		},
		{
			name:         "ipv6-multi-header-2",
			headers:      map[string]string{"forwarded-for": ipv6Global, "x-forwarded-for": "2001:0db8:2001:zzzz::"},
			expectedIP:   netaddrIP{},
			multiHeaders: "x-forwarded-for,forwarded-for",
		},
	}, tcs...)
	tcs = append([]ipTestCase{
		{
			name:       "no-headers",
			expectedIP: netaddrIP{},
		},
		{
			name:       "header-case",
			expectedIP: netaddrMustParseIP(ipv4Global),
			headers:    map[string]string{"X-fOrWaRdEd-FoR": ipv4Global},
		},
		{
			name:           "user-header",
			expectedIP:     netaddrMustParseIP(ipv4Global),
			headers:        map[string]string{"x-forwarded-for": ipv6Global, "custom-header": ipv4Global},
			clientIPHeader: "custom-header",
		},
		{
			name:           "user-header-not-found",
			expectedIP:     netaddrIP{},
			headers:        map[string]string{"x-forwarded-for": ipv4Global},
			clientIPHeader: "custom-header",
		},
	}, tcs...)

	return tcs
}

type mockspan struct {
	tags map[string]interface{}
}

func (m *mockspan) SetTag(tag string, value interface{}) {
	if m.tags == nil {
		m.tags = make(map[string]interface{})
	}
	m.tags[tag] = value
}

func (m *mockspan) Tag(tag string) interface{} {
	if m.tags == nil {
		return nil
	}
	return m.tags[tag]
}

func TestIPHeaders(t *testing.T) {
	// Make sure to restore the real value of clientIPHeader at the end of the test
	defer func(s string) { clientIPHeader = s }(clientIPHeader)
	for _, tc := range genIPTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			header := http.Header{}
			for k, v := range tc.headers {
				header.Add(k, v)
			}
			r := http.Request{Header: header, RemoteAddr: tc.remoteAddr}
			clientIPHeader = tc.clientIPHeader
			var span mockspan
			SetIPTags(&span, &r)
			if tc.expectedIP.IsValid() {
				require.Equal(t, tc.expectedIP.String(), span.Tag(ext.HTTPClientIP))
				require.Nil(t, span.Tag(multipleIPHeaders))
			} else {
				require.Nil(t, span.Tag(ext.HTTPClientIP))
				if tc.multiHeaders != "" {
					require.Equal(t, tc.multiHeaders, span.Tag(multipleIPHeaders))
					for hdr, ip := range tc.headers {
						require.Equal(t, ip, span.Tag(ext.HTTPRequestHeaders+"."+hdr))
					}
				}
			}
		})
	}
}

func randIPv4() netaddrIP {
	return netaddrIPv4(uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()))
}

func randIPv6() netaddrIP {
	return netaddrIPv6Raw([16]byte{
		uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()),
		uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()),
		uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()),
		uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()), uint8(rand.Uint32()),
	})
}

func randGlobalIPv4() netaddrIP {
	for {
		ip := randIPv4()
		if isGlobal(ip) {
			return ip
		}
	}
}

func randGlobalIPv6() netaddrIP {
	for {
		ip := randIPv6()
		if isGlobal(ip) {
			return ip
		}
	}
}

func randPrivateIPv4() netaddrIP {
	for {
		ip := randIPv4()
		if !isGlobal(ip) && ip.IsPrivate() {
			return ip
		}
	}
}

func randPrivateIPv6() netaddrIP {
	for {
		ip := randIPv6()
		if !isGlobal(ip) && ip.IsPrivate() {
			return ip
		}
	}
}
