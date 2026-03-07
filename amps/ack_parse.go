package amps

func parseAckBytes(ackType []byte) int {
	var ack int
	start := 0
	for start <= len(ackType) {
		end := start
		for end < len(ackType) && ackType[end] != ',' {
			end++
		}
		token := ackType[start:end]
		switch len(token) {
		case 5:
			if bytesEqualString(token, "stats") {
				ack |= AckTypeStats
			}
		case 6:
			if bytesEqualString(token, "parsed") {
				ack |= AckTypeParsed
			}
		case 8:
			if bytesEqualString(token, "received") {
				ack |= AckTypeReceived
			}
		case 9:
			switch {
			case bytesEqualString(token, "persisted"):
				ack |= AckTypePersisted
			case bytesEqualString(token, "completed"):
				ack |= AckTypeCompleted
			case bytesEqualString(token, "processed"):
				ack |= AckTypeProcessed
			}
		}

		if end == len(ackType) {
			break
		}
		start = end + 1
	}
	return ack
}
