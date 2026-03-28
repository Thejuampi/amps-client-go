package amps

const maxStoreSequence = ^uint64(0)

func deriveNextStoreSequence(maxSeen uint64, storedNext uint64) (uint64, bool) {
	if maxSeen == maxStoreSequence {
		return maxStoreSequence, true
	}
	if storedNext == 0 || storedNext <= maxSeen {
		return maxSeen + 1, false
	}
	return storedNext, false
}

func advanceStoreSequence(sequence uint64) (uint64, bool) {
	if sequence == maxStoreSequence {
		return maxStoreSequence, true
	}
	return sequence + 1, false
}
