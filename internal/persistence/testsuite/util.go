package testsuite

func bytea(strs ...string) [][]byte {
	a := make([][]byte, len(strs))
	for i, str := range strs {
		a[i] = []byte(str)
	}
	return a
}
