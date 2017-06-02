package models

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import "github.com/tinylib/msgp/msgp"

// DecodeMsg implements msgp.Decodable
func (z *Row) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
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
		case "name":
			z.Name, err = dc.ReadString()
			if err != nil {
				return
			}
		case "tags":
			var zhct uint32
			zhct, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Tags == nil && zhct > 0 {
				z.Tags = make(map[string]string, zhct)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zhct > 0 {
				zhct--
				var zxvk string
				var zbzg string
				zxvk, err = dc.ReadString()
				if err != nil {
					return
				}
				zbzg, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Tags[zxvk] = zbzg
			}
		case "columns":
			var zcua uint32
			zcua, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Columns) >= int(zcua) {
				z.Columns = (z.Columns)[:zcua]
			} else {
				z.Columns = make([]string, zcua)
			}
			for zbai := range z.Columns {
				z.Columns[zbai], err = dc.ReadString()
				if err != nil {
					return
				}
			}
		case "values":
			var zxhx uint32
			zxhx, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Values) >= int(zxhx) {
				z.Values = (z.Values)[:zxhx]
			} else {
				z.Values = make([][]interface{}, zxhx)
			}
			for zcmr := range z.Values {
				var zlqf uint32
				zlqf, err = dc.ReadArrayHeader()
				if err != nil {
					return
				}
				if cap(z.Values[zcmr]) >= int(zlqf) {
					z.Values[zcmr] = (z.Values[zcmr])[:zlqf]
				} else {
					z.Values[zcmr] = make([]interface{}, zlqf)
				}
				for zajw := range z.Values[zcmr] {
					z.Values[zcmr][zajw], err = dc.ReadIntf()
					if err != nil {
						return
					}
				}
			}
		case "partial":
			z.Partial, err = dc.ReadBool()
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
func (z *Row) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 5
	// write "name"
	err = en.Append(0x85, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Name)
	if err != nil {
		return
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
	for zxvk, zbzg := range z.Tags {
		err = en.WriteString(zxvk)
		if err != nil {
			return
		}
		err = en.WriteString(zbzg)
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
	for zbai := range z.Columns {
		err = en.WriteString(z.Columns[zbai])
		if err != nil {
			return
		}
	}
	// write "values"
	err = en.Append(0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Values)))
	if err != nil {
		return
	}
	for zcmr := range z.Values {
		err = en.WriteArrayHeader(uint32(len(z.Values[zcmr])))
		if err != nil {
			return
		}
		for zajw := range z.Values[zcmr] {
			err = en.WriteIntf(z.Values[zcmr][zajw])
			if err != nil {
				return
			}
		}
	}
	// write "partial"
	err = en.Append(0xa7, 0x70, 0x61, 0x72, 0x74, 0x69, 0x61, 0x6c)
	if err != nil {
		return err
	}
	err = en.WriteBool(z.Partial)
	if err != nil {
		return
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *Row) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Name) + 5 + msgp.MapHeaderSize
	if z.Tags != nil {
		for zxvk, zbzg := range z.Tags {
			_ = zbzg
			s += msgp.StringPrefixSize + len(zxvk) + msgp.StringPrefixSize + len(zbzg)
		}
	}
	s += 8 + msgp.ArrayHeaderSize
	for zbai := range z.Columns {
		s += msgp.StringPrefixSize + len(z.Columns[zbai])
	}
	s += 7 + msgp.ArrayHeaderSize
	for zcmr := range z.Values {
		s += msgp.ArrayHeaderSize
		for zajw := range z.Values[zcmr] {
			s += msgp.GuessSize(z.Values[zcmr][zajw])
		}
	}
	s += 8 + msgp.BoolSize
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Rows) DecodeMsg(dc *msgp.Reader) (err error) {
	var zjfb uint32
	zjfb, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zjfb) {
		(*z) = (*z)[:zjfb]
	} else {
		(*z) = make(Rows, zjfb)
	}
	for zpks := range *z {
		if dc.IsNil() {
			err = dc.ReadNil()
			if err != nil {
				return
			}
			(*z)[zpks] = nil
		} else {
			if (*z)[zpks] == nil {
				(*z)[zpks] = new(Row)
			}
			err = (*z)[zpks].DecodeMsg(dc)
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Rows) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for zcxo := range z {
		if z[zcxo] == nil {
			err = en.WriteNil()
			if err != nil {
				return
			}
		} else {
			err = z[zcxo].EncodeMsg(en)
			if err != nil {
				return
			}
		}
	}
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Rows) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zcxo := range z {
		if z[zcxo] == nil {
			s += msgp.NilSize
		} else {
			s += z[zcxo].Msgsize()
		}
	}
	return
}
