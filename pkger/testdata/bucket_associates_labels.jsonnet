local Label(name, desc, color) = {
    apiVersion: 'influxdata.com/v2alpha1',
    kind: 'Label',
    metadata: {
        name: name
    },
    spec: {
        description: desc,
        color: color
    }
};

local LabelAssociations(names=[]) = [
    {kind: 'Label', name: name}
    for name in names
];

local Bucket(name, desc, secs, associations=LabelAssociations(['label-1'])) = {
    apiVersion: 'influxdata.com/v2alpha1',
    kind: 'Bucket',
    metadata: {
        name: name
    },
    spec: {
        description: desc,
        retentionRules: [
            {type: 'expire', everySeconds:  secs}
        ],
        associations: associations
    }
};

[
    Label("label-1",desc="desc_1", color='#eee888'),
    Bucket(name="rucket-1", desc="desc_1", secs=10000),
    Bucket("rucket-2", "desc-2", 20000),
    Bucket("rucket-3", "desc_3", 30000),
]
