package bookmark

func NormalizeSubID(subIDs string) string {
	start := 0
	for start <= len(subIDs) {
		end := len(subIDs)
		for index := start; index < len(subIDs); index++ {
			if subIDs[index] == ',' {
				end = index
				break
			}
		}

		tokenStart := start
		for tokenStart < end {
			switch subIDs[tokenStart] {
			case ' ', '\t', '\n', '\r':
				tokenStart++
			default:
				goto trimTokenEnd
			}
		}
		tokenStart = end

	trimTokenEnd:
		tokenEnd := end
		for tokenEnd > tokenStart {
			switch subIDs[tokenEnd-1] {
			case ' ', '\t', '\n', '\r':
				tokenEnd--
			default:
				return subIDs[tokenStart:tokenEnd]
			}
		}

		if end == len(subIDs) {
			break
		}
		start = end + 1
	}
	return ""
}
