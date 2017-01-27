export default function sources(state = [], action) {
  switch (action.type) {
    case 'LOAD_SOURCES': {
      return action.payload.sources;
    }

    case 'SOURCE_UPDATED': {
      const {source} = action.payload;
      const updatedIndex = state.findIndex((s) => s.id === source.id);
      const updatedSources = source.default ? state.map((s) => {
        s.default = false; return s;
      }) : [...state];
      updatedSources[updatedIndex] = source;
      return updatedSources;
    }

    case 'SOURCE_REMOVED': {
      const {source} = action.payload;
      const updatedSources = state.filter((s) => s.id !== source.id);
      return updatedSources;
    }

    case 'SOURCE_ADDED': {
      const {source} = action.payload;
      const updatedSources = source.default ? state.map((s) => {
        s.default = false; return s;
      }) : state;
      return [...updatedSources, source];
    }
  }

  return state;
}
