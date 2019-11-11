package main

func checkIdentifier(n string) bool {
	if len(n) == 0 {
		return false
	}
	for _, r := range n {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r == '_' || r == '.':
		default:
			return false
		}
	}
	return true
}
