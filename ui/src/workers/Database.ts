import levelup from 'levelup'
import encoding from 'encoding-down'
import level from 'level-js'

export default levelup(encoding(level('worker'), {valueEncoding: 'json'}))
