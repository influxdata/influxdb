export function loadSources(sources) {
  return {
    type: 'LOAD_SOURCES',
    payload: {
      sources,
    },
  };
}

export function updateSource(source) {
  return {
    type: 'SOURCE_UPDATED',
    payload: {
      source,
    },
  };
}

export function removeSource(source) {
  return {
    type: 'SOURCE_REMOVED',
    payload: {
      source,
    },
  };
}

export function addSource(source) {
  return {
    type: 'SOURCE_ADDED',
    payload: {
      source,
    },
  };
}
