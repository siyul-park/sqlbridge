package compile

import (
	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/instr"
)

// Label is a deferred jump target within a Builder's instruction stream.
type Label int

// Builder assembles a minivm bytecode sequence with label-based branches.
// Branch instructions are emitted with placeholder operands and patched to
// signed byte-relative offsets in Build, so callers never compute offsets by
// hand. This is the foundation for emitting a whole SQL statement as a single
// minivm program.
type Builder struct {
	insts  []instr.Instruction
	labels map[Label]int
	fixups []fixup
	next   Label
}

type fixup struct {
	idx   int // index of the branch instruction in insts
	op    instr.Opcode
	label Label
}

// NewBuilder returns an empty Builder.
func NewBuilder() *Builder {
	return &Builder{labels: make(map[Label]int)}
}

// Emit appends a non-branch instruction.
func (b *Builder) Emit(op instr.Opcode, operands ...uint64) *Builder {
	b.insts = append(b.insts, instr.New(op, operands...))
	return b
}

// Label allocates a fresh, unbound label.
func (b *Builder) Label() Label {
	l := b.next
	b.next++
	return l
}

// Bind anchors a label at the current end of the instruction stream.
func (b *Builder) Bind(l Label) *Builder {
	b.labels[l] = len(b.insts)
	return b
}

// Branch emits a branch instruction (BR or BR_IF) targeting a label. The byte
// offset is resolved in Build.
func (b *Builder) Branch(op instr.Opcode, l Label) *Builder {
	b.fixups = append(b.fixups, fixup{idx: len(b.insts), op: op, label: l})
	b.insts = append(b.insts, instr.New(op, 0))
	return b
}

// Len reports the number of instructions emitted so far.
func (b *Builder) Len() int { return len(b.insts) }

// Build resolves all branches and returns the finished instruction sequence.
func (b *Builder) Build() ([]instr.Instruction, error) {
	// Byte offset of every instruction index, plus the end-of-stream offset.
	offsets := make([]int, len(b.insts)+1)
	for i, in := range b.insts {
		offsets[i+1] = offsets[i] + in.Width()
	}

	for _, f := range b.fixups {
		target, ok := b.labels[f.label]
		if !ok {
			return nil, errors.Errorf("compile: unbound label %d", f.label)
		}
		end := offsets[f.idx] + b.insts[f.idx].Width()
		rel := offsets[target] - end
		if rel < -0x8000 || rel > 0x7FFF {
			return nil, errors.Errorf("compile: branch offset %d out of int16 range", rel)
		}
		b.insts[f.idx] = instr.New(f.op, uint64(uint16(int16(rel))))
	}

	return b.insts, nil
}
