package eval

func NewSubstr() Function {
	return func(args []Value) (Value, error) {
		if len(args) == 0 {
			return NewString(""), nil
		}

		str, err := ToString(args[0])
		if err != nil {
			return nil, err
		}

		offset, length := int64(0), int64(len(str))
		if len(args) > 1 {
			if offset, err = ToInt(args[1]); err != nil {
				return nil, err
			}
		}
		if len(args) > 2 {
			if length, err = ToInt(args[2]); err != nil {
				return nil, err
			}
		}

		offset = (offset + int64(len(str))) % int64(len(str))
		if offset < 0 {
			offset += int64(len(str))
		}
		length = max(0, length)
		if offset+length > int64(len(str)) {
			length = int64(len(str)) - offset
		}
		return NewString(str[offset : offset+length]), nil
	}
}
