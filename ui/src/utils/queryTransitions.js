import defaultQueryConfig from './defaultQueryConfig';

export function chooseNamespace(query, namespace) {
  return Object.assign({}, query, namespace);
}

export function chooseMeasurement(query, measurement) {
  return Object.assign({}, defaultQueryConfig(query.id), {
    database: query.database,
    retentionPolicy: query.retentionPolicy,
    measurement,
  });
}

export function toggleField(query, {field, funcs}) {
  const isSelected = query.fields.find((f) => f.field === field);
  if (isSelected) {
    const nextFields = query.fields.filter((f) => f.field !== field);
    if (!nextFields.length) {
      const nextGroupBy = Object.assign({}, query.groupBy, {time: null});
      return Object.assign({}, query, {
        fields: nextFields,
        groupBy: nextGroupBy,
      });
    }

    return Object.assign({}, query, {
      fields: nextFields,
    });
  }

  return Object.assign({}, query, {
    fields: query.fields.concat({field, funcs}),
  });
}
