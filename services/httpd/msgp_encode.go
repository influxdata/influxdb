package httpd

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Column) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zxvk uint32
	zxvk, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zxvk > 0 {
		zxvk--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "name":
			z.Name, err = dc.ReadString()
			if err != nil {
				return
			}
		case "type":
			z.Type, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Column) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "name"
	err = en.Append(0x82, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Name)
	if err != nil {
		return
	}
	// write "type"
	err = en.Append(0xa4, 0x74, 0x79, 0x70, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Type)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Column) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Name) + 5 + msgp.StringPrefixSize + len(z.Type)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Message) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zbzg uint32
	zbzg, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zbzg > 0 {
		zbzg--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "level":
			z.Level, err = dc.ReadString()
			if err != nil {
				return
			}
		case "text":
			z.Text, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Message) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "level"
	err = en.Append(0x82, 0xa5, 0x6c, 0x65, 0x76, 0x65, 0x6c)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Level)
	if err != nil {
		return
	}
	// write "text"
	err = en.Append(0xa4, 0x74, 0x65, 0x78, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Text)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Message) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Level) + 5 + msgp.StringPrefixSize + len(z.Text)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *ResultHeader) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zcmr uint32
	zcmr, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zcmr > 0 {
		zcmr--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "id":
			z.ID, err = dc.ReadInt()
			if err != nil {
				return
			}
		case "messages":
			var zajw uint32
			zajw, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Messages) >= int(zajw) {
				z.Messages = (z.Messages)[:zajw]
			} else {
				z.Messages = make([]Message, zajw)
			}
			for zbai := range z.Messages {
				var zwht uint32
				zwht, err = dc.ReadMapHeader()
				if err != nil {
					return
				}
				for zwht > 0 {
					zwht--
					field, err = dc.ReadMapKeyPtr()
					if err != nil {
						return
					}
					switch msgp.UnsafeString(field) {
					case "level":
						z.Messages[zbai].Level, err = dc.ReadString()
						if err != nil {
							return
						}
					case "text":
						z.Messages[zbai].Text, err = dc.ReadString()
						if err != nil {
							return
						}
					default:
						err = dc.Skip()
						if err != nil {
							return
						}
					}
				}
			}
		case "error":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					return
				}
				z.Error = nil
			} else {
				if z.Error == nil {
					z.Error = new(string)
				}
				*z.Error, err = dc.ReadString()
				if err != nil {
					return
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *ResultHeader) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "id"
	err = en.Append(0x83, 0xa2, 0x69, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteInt(z.ID)
	if err != nil {
		return
	}
	// write "messages"
	err = en.Append(0xa8, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Messages)))
	if err != nil {
		return
	}
	for zbai := range z.Messages {
		// map header, size 2
		// write "level"
		err = en.Append(0x82, 0xa5, 0x6c, 0x65, 0x76, 0x65, 0x6c)
		if err != nil {
			return err
		}
		err = en.WriteString(z.Messages[zbai].Level)
		if err != nil {
			return
		}
		// write "text"
		err = en.Append(0xa4, 0x74, 0x65, 0x78, 0x74)
		if err != nil {
			return err
		}
		err = en.WriteString(z.Messages[zbai].Text)
		if err != nil {
			return
		}
	}
	// write "error"
	err = en.Append(0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	if err != nil {
		return err
	}
	if z.Error == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		err = en.WriteString(*z.Error)
		if err != nil {
			return
		}
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *ResultHeader) Msgsize() (s int) {
	s = 1 + 3 + msgp.IntSize + 9 + msgp.ArrayHeaderSize
	for zbai := range z.Messages {
		s += 1 + 6 + msgp.StringPrefixSize + len(z.Messages[zbai].Level) + 5 + msgp.StringPrefixSize + len(z.Messages[zbai].Text)
	}
	s += 6
	if z.Error == nil {
		s += msgp.NilSize
	} else {
		s += msgp.StringPrefixSize + len(*z.Error)
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Row) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zcua uint32
	zcua, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zcua > 0 {
		zcua--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "values":
			var zxhx uint32
			zxhx, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Value) >= int(zxhx) {
				z.Value = (z.Value)[:zxhx]
			} else {
				z.Value = make([]interface{}, zxhx)
			}
			for zhct := range z.Value {
				z.Value[zhct], err = dc.ReadIntf()
				if err != nil {
					return
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *Row) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "values"
	err = en.Append(0x81, 0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Value)))
	if err != nil {
		return
	}
	for zhct := range z.Value {
		err = en.WriteIntf(z.Value[zhct])
		if err != nil {
			return
		}
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Row) Msgsize() (s int) {
	s = 1 + 7 + msgp.ArrayHeaderSize
	for zhct := range z.Value {
		s += msgp.GuessSize(z.Value[zhct])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *RowBatchError) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zlqf uint32
	zlqf, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zlqf > 0 {
		zlqf--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "error":
			z.Error, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z RowBatchError) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "error"
	err = en.Append(0x81, 0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Error)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z RowBatchError) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Error)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *RowBatchHeader) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zdaf uint32
	zdaf, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zdaf > 0 {
		zdaf--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "length":
			z.Length, err = dc.ReadInt()
			if err != nil {
				return
			}
		case "continue":
			z.Continue, err = dc.ReadBool()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z RowBatchHeader) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "length"
	err = en.Append(0x82, 0xa6, 0x6c, 0x65, 0x6e, 0x67, 0x74, 0x68)
	if err != nil {
		return err
	}
	err = en.WriteInt(z.Length)
	if err != nil {
		return
	}
	// write "continue"
	err = en.Append(0xa8, 0x63, 0x6f, 0x6e, 0x74, 0x69, 0x6e, 0x75, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteBool(z.Continue)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z RowBatchHeader) Msgsize() (s int) {
	s = 1 + 7 + msgp.IntSize + 9 + msgp.BoolSize
	return
}

// DecodeMsg implements msgp.Decodable
func (z *RowError) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zpks uint32
	zpks, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zpks > 0 {
		zpks--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "error":
			z.Error, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z RowError) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "error"
	err = en.Append(0x81, 0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Error)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z RowError) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Error)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesError) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zjfb uint32
	zjfb, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zjfb > 0 {
		zjfb--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "error":
			z.Error, err = dc.ReadString()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z SeriesError) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "error"
	err = en.Append(0x81, 0xa5, 0x65, 0x72, 0x72, 0x6f, 0x72)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Error)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z SeriesError) Msgsize() (s int) {
	s = 1 + 6 + msgp.StringPrefixSize + len(z.Error)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesHeader) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zxpk uint32
	zxpk, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zxpk > 0 {
		zxpk--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "name":
			if dc.IsNil() {
				err = dc.ReadNil()
				if err != nil {
					return
				}
				z.Name = nil
			} else {
				if z.Name == nil {
					z.Name = new(string)
				}
				*z.Name, err = dc.ReadString()
				if err != nil {
					return
				}
			}
		case "tags":
			var zdnj uint32
			zdnj, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Tags == nil && zdnj > 0 {
				z.Tags = make(map[string]string, zdnj)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zdnj > 0 {
				zdnj--
				var zcxo string
				var zeff string
				zcxo, err = dc.ReadString()
				if err != nil {
					return
				}
				zeff, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Tags[zcxo] = zeff
			}
		case "columns":
			var zobc uint32
			zobc, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Columns) >= int(zobc) {
				z.Columns = (z.Columns)[:zobc]
			} else {
				z.Columns = make([]Column, zobc)
			}
			for zrsw := range z.Columns {
				var zsnv uint32
				zsnv, err = dc.ReadMapHeader()
				if err != nil {
					return
				}
				for zsnv > 0 {
					zsnv--
					field, err = dc.ReadMapKeyPtr()
					if err != nil {
						return
					}
					switch msgp.UnsafeString(field) {
					case "name":
						z.Columns[zrsw].Name, err = dc.ReadString()
						if err != nil {
							return
						}
					case "type":
						z.Columns[zrsw].Type, err = dc.ReadString()
						if err != nil {
							return
						}
					default:
						err = dc.Skip()
						if err != nil {
							return
						}
					}
				}
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *SeriesHeader) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "name"
	err = en.Append(0x83, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return err
	}
	if z.Name == nil {
		err = en.WriteNil()
		if err != nil {
			return
		}
	} else {
		err = en.WriteString(*z.Name)
		if err != nil {
			return
		}
	}
	// write "tags"
	err = en.Append(0xa4, 0x74, 0x61, 0x67, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for zcxo, zeff := range z.Tags {
		err = en.WriteString(zcxo)
		if err != nil {
			return
		}
		err = en.WriteString(zeff)
		if err != nil {
			return
		}
	}
	// write "columns"
	err = en.Append(0xa7, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Columns)))
	if err != nil {
		return
	}
	for zrsw := range z.Columns {
		// map header, size 2
		// write "name"
		err = en.Append(0x82, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
		if err != nil {
			return err
		}
		err = en.WriteString(z.Columns[zrsw].Name)
		if err != nil {
			return
		}
		// write "type"
		err = en.Append(0xa4, 0x74, 0x79, 0x70, 0x65)
		if err != nil {
			return err
		}
		err = en.WriteString(z.Columns[zrsw].Type)
		if err != nil {
			return
		}
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *SeriesHeader) Msgsize() (s int) {
	s = 1 + 5
	if z.Name == nil {
		s += msgp.NilSize
	} else {
		s += msgp.StringPrefixSize + len(*z.Name)
	}
	s += 5 + msgp.MapHeaderSize
	if z.Tags != nil {
		for zcxo, zeff := range z.Tags {
			_ = zeff
			s += msgp.StringPrefixSize + len(zcxo) + msgp.StringPrefixSize + len(zeff)
		}
	}
	s += 8 + msgp.ArrayHeaderSize
	for zrsw := range z.Columns {
		s += 1 + 5 + msgp.StringPrefixSize + len(z.Columns[zrsw].Name) + 5 + msgp.StringPrefixSize + len(z.Columns[zrsw].Type)
	}
	return
}
