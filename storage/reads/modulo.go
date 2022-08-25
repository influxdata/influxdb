package reads

func Modulo(dividend, modulus int64) int64 {
	r := dividend % modulus
	if r < 0 {
		r += modulus
	}
	return r
}

// WindowStart calculates the start time of a window given a timestamp,
// the window period (every), and the offset starting from the epoch.
//
// Note that the normalized offset value can fall on either side of the
// normalized timestamp. If it lies to the left we know it represents
// the start time. Otherwise it represents the stop time, in which case
// we decrement by the window period to get the start time.
func WindowStart(t, every, offset int64) int64 {
	mod := Modulo(t, every)
	off := Modulo(offset, every)
	beg := t - mod + off
	if mod < off {
		beg -= every
	}
	return beg
}

// WindowStop calculates the stop time of a window given a timestamp,
// the window period (every), and the offset starting from the epoch.
//
// Note that the normalized offset value can fall on either side of the
// normalized timestamp. If it lies to the right we know it represents
// the stop time. Otherwise it represents the start time, in which case
// we increment by the window period to get the stop time.
func WindowStop(t, every, offset int64) int64 {
	mod := Modulo(t, every)
	off := Modulo(offset, every)
	end := t - mod + off
	if mod >= off {
		end += every
	}
	return end
}
