package main

func main() {
	// in, err := os.OpenFile("/tmp/log.modified", os.O_RDONLY, 0)
	// if err != nil {
	// 	panic(err)
	// }
	// defer in.Close()
	// out, err := os.OpenFile("/tmp/log.modified.2", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	// if err != nil {
	// 	panic(err)
	// }
	// defer out.Close()
	// for index := 1; ; index++ {
	// 	entry, _ := raft.NewLogEntry(nil, nil, 0, 0, nil)
	// 	_, err = entry.Decode(in)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	entry.SetIndex(uint64(index))
	// 	_, err = entry.Encode(out)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("index: %d\n", index)
	// }
}
