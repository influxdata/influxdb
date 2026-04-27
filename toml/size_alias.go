package toml

// Size and SSize are aliases for the branch-appropriate size-parser
// implementation. On the 1.x branch they point at SizeV1/SSizeV1, which
// preserve the historical binary meaning of bare 'k'/'m'/'g' suffixes on
// top of the richer humanize vocabulary. On the 2.x branch they point at
// SizeV2/SSizeV2, which use humanize's native SI-decimal interpretation of
// bare letters.
//
// This file is the only intentional point of divergence between the 1.x
// and 2.x branches in the toml package — everything else in the package is
// shared. Do NOT cherry-pick this file across branches.
type (
	Size  = SizeV1
	SSize = SSizeV1
)
