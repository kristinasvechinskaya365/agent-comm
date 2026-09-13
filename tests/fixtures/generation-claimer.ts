import { StateService } from '../../src/domain/state.js';
import { createDb } from '../../src/storage/database.js';

const [path, owner, mode = 'claim'] = process.argv.slice(2);

try {
  const db = createDb({ path });
  const state = new StateService(db);
  const result =
    mode === 'get'
      ? state.getVersioned('race', 'expiry')
      : state.compareGeneration(
          'race',
          mode === 'claim' ? 'claim' : 'transition',
          mode === 'claim' ? 0 : 1,
          mode === 'delete'
            ? { type: 'delete' }
            : { type: 'set', value: mode === 'claim' ? 'claimed' : owner, updatedBy: owner },
        );
  db.close();
  process.stdout.write(JSON.stringify(result));
} catch (error) {
  process.stderr.write(error instanceof Error ? (error.stack ?? error.message) : String(error));
  process.exitCode = 1;
}
