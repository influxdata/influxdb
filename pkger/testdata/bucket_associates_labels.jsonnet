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

local Bucket(name, desc, secs, associations=LabelAssociations(['label_1'])) = {
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
    Label("label_1",desc="desc_1", color='#eee888'),
    Bucket(name="rucket_1", desc="desc_1", secs=10000),
    Bucket("rucket_2", "desc_2", 20000),
    Bucket("rucket_3", "desc_3", 30000),
]
