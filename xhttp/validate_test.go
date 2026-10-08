package xhttp

import "testing"

func TestValidateAcceptsValid(t *testing.T) {
	cases := []struct {
		name string
		o    Options
	}{
		{"empty", Options{}},
		{"packet-up", Options{Mode: ModePacketUp}},
		{"stream-up", Options{Mode: ModeStreamUp}},
		{"gRPC stream-up", Options{Mode: ModeStreamUp, GRPCFraming: true, Path: "/example.Tunnel"}},
		{"gRPC stream-one", Options{Mode: ModeStreamOne, GRPCFraming: true}},
		{"gRPC auto server", Options{Mode: ModeAuto, GRPCFraming: true}},
		{"all meta placements distinct", Options{
			SessionPlacement: PlacementHeader,
			SeqPlacement:     PlacementCookie,
		}},
		{"same non-path placement, distinct keys", Options{
			SessionPlacement: PlacementHeader, SessionKey: "X-A",
			SeqPlacement: PlacementHeader, SeqKey: "X-B",
		}},
		{"both seq+session on path", Options{
			SessionPlacement: PlacementPath,
			SeqPlacement:     PlacementPath,
		}},
		{"obfs header repeat-x", Options{
			XPaddingObfsMode:  true,
			XPaddingPlacement: PlacementHeader,
			XPaddingMethod:    PaddingMethodRepeatX,
		}},
		{"obfs cookie tokenish", Options{
			XPaddingObfsMode:  true,
			XPaddingPlacement: PlacementCookie,
			XPaddingMethod:    PaddingMethodTokenish,
		}},
		{"ranges with To==0 are unset", Options{
			XPaddingBytes:      &Range{From: 500, To: 0},
			ScMaxEachPostBytes: &Range{From: 100, To: 0},
		}},
		{"valid xmux", Options{Xmux: &XmuxConfig{
			MaxConcurrency: &Range{From: 16, To: 16},
			MaxConnections: &Range{From: 4, To: 4},
		}}},
		{"uplink header placement", Options{
			UplinkDataPlacement: PlacementHeader,
			UplinkDataKey:       "payload",
		}},
		{"uplink cookie placement", Options{
			UplinkDataPlacement: PlacementCookie,
			UplinkDataKey:       "payload",
		}},
		{"uplink auto placement", Options{
			UplinkDataPlacement: PlacementAuto,
			UplinkDataKey:       "payload",
		}},
		{"uplink body placement (no key needed)", Options{
			UplinkDataPlacement: PlacementBody,
		}},
		{"custom session ID", Options{
			SessionIDTable:  "Base62",
			SessionIDLength: &Range{From: 16, To: 16},
		}},
		{"stream-one mode", Options{Mode: ModeStreamOne}},
		{"auto mode", Options{Mode: ModeAuto}},
		{"server max header bytes", Options{ServerMaxHeaderBytes: 16384}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.o.Validate(); err != nil {
				t.Fatalf("expected valid, got error: %v", err)
			}
		})
	}
}

func TestValidateRejectsInvalid(t *testing.T) {
	cases := []struct {
		name string
		o    Options
	}{
		{"bad mode", Options{Mode: "stream-two"}},
		{"gRPC packet-up", Options{Mode: ModePacketUp, GRPCFraming: true}},
		{"gRPC default mode", Options{GRPCFraming: true}},
		{"gRPC without headers", Options{Mode: ModeStreamOne, GRPCFraming: true, NoGRPCHeader: true}},
		{"gRPC GET", Options{Mode: ModeStreamOne, GRPCFraming: true, Method: "GET"}},
		{"gRPC query path", Options{Mode: ModeStreamOne, GRPCFraming: true, Path: "/example?x=1"}},
		{"gRPC nested path", Options{Mode: ModeStreamOne, GRPCFraming: true, Path: "/example/nested"}},
		{"gRPC duplicate slash", Options{Mode: ModeStreamOne, GRPCFraming: true, Path: "//example//"}},
		{"gRPC invalid service name", Options{Mode: ModeStreamOne, GRPCFraming: true, Path: "/example..Tunnel"}},
		{"gRPC header upload", Options{Mode: ModeStreamUp, GRPCFraming: true, UplinkDataPlacement: PlacementHeader, UplinkDataKey: "X-Up"}},
		{"gRPC query padding", Options{Mode: ModeStreamUp, GRPCFraming: true, XPaddingObfsMode: true, XPaddingPlacement: PlacementQuery}},
		{"bad session placement", Options{SessionPlacement: "body"}},
		{"bad seq placement", Options{SeqPlacement: "nonsense"}},
		{"session/seq collide on header default keys", Options{
			SessionPlacement: PlacementQuery,
			SeqPlacement:     PlacementQuery,
			SessionKey:       "dup",
			SeqKey:           "dup",
		}},
		{"obfs bad placement (path not allowed)", Options{
			XPaddingObfsMode:  true,
			XPaddingPlacement: PlacementPath,
		}},
		{"obfs bad method", Options{
			XPaddingObfsMode:  true,
			XPaddingPlacement: PlacementHeader,
			XPaddingMethod:    "rot13",
		}},
		{"negative range From", Options{XPaddingBytes: &Range{From: -1, To: 100}}},
		{"To < From", Options{ScMaxEachPostBytes: &Range{From: 1000, To: 500}}},
		{"negative buffered posts", Options{ScMaxBufferedPosts: -5}},
		{"bad xmux range", Options{Xmux: &XmuxConfig{
			MaxConnections: &Range{From: 10, To: 2},
		}}},
		{"uplink header without key", Options{UplinkDataPlacement: PlacementHeader}},
		{"uplink cookie without key", Options{UplinkDataPlacement: PlacementCookie}},
		{"uplink auto without key", Options{UplinkDataPlacement: PlacementAuto}},
		{"uplink bad placement", Options{UplinkDataPlacement: "path"}},
		{"negative server max header bytes", Options{ServerMaxHeaderBytes: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.o.Validate(); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestValidateObfsMethodIgnoredWhenObfsOff(t *testing.T) {
	// When obfs is off, XPaddingMethod/Placement are not consulted, so an
	// otherwise-invalid method must not cause rejection.
	o := Options{XPaddingObfsMode: false, XPaddingMethod: "rot13", XPaddingPlacement: "path"}
	if err := o.Validate(); err != nil {
		t.Fatalf("obfs-off should ignore padding method/placement, got %v", err)
	}
}
