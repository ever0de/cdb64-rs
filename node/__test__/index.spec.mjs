import test from 'ava'

import { CdbWriter, Cdb } from '../index.js'
import { tmpdir } from 'os'
import { join } from 'path'
import { randomBytes } from 'crypto'
import { unlinkSync } from 'fs'

test('CdbWriter/Cdb native roundtrip', (t) => {
  const dbPath = join(tmpdir(), 'test-cdb-' + randomBytes(8).toString('hex') + '.cdb')
  const writer = new CdbWriter(dbPath)
  writer.put(Buffer.from('foo'), Buffer.from('bar'))
  writer.put(Buffer.from('baz'), Buffer.from('qux'))
  writer.finalize()

  const cdb = Cdb.open(dbPath)
  t.deepEqual(cdb.get(Buffer.from('foo')), Buffer.from('bar'))
  t.deepEqual(cdb.get(Buffer.from('baz')), Buffer.from('qux'))
  t.is(cdb.get(Buffer.from('notfound')), null)

  const all = cdb.iter()
  const keys = all.map(e => Buffer.from(e.key).toString())
  const values = all.map(e => Buffer.from(e.value).toString())
  t.true(keys.includes('foo'))
  t.true(keys.includes('baz'))
  t.true(values.includes('bar'))
  t.true(values.includes('qux'))

  unlinkSync(dbPath)
})
