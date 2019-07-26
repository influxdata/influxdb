// A dependency graph that contains any wasm must all be imported
// asynchronously. This `bootstrap.ts` file does the single async import, so
// that no one else needs to worry about it again.
import('./index').catch(e => console.error('Error importing `index.tsx`:', e))
