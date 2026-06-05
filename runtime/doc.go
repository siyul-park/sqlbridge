// Package runtime bridges SQLBridge onto the minivm bytecode VM
// (github.com/siyul-park/minivm). It owns VM construction, the SQL
// value <-> minivm Boxed boxing layer, and the host-function registry
// through which compiled query programs reach storage backends and SQL
// value semantics.
//
// This file currently only anchors the package; the integration spike
// in runtime_test.go validates the minivm API surface the rest of the
// rebuild depends on.
package runtime
