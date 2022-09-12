import "array"
import "profiler"
import "internal/gen"
import "runtime"

option profiler.enabledProfilers = ["operator"]

array.from(rows: [{version: runtime.version()}])
