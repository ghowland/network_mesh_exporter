package main

import "encoding/json"

// jsonUnmarshal keeps the encoding/json import local to this file, so
// that main.go does not carry it alongside the protocol types.
func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

