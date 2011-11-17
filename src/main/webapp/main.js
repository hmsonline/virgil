Ext.require(['Ext.data.*', 'Ext.grid.*']);

Ext.Ajax.defaultHeaders = {
    'Accept': 'application/json'
};

Ext.onReady(function() {
    Ext.define('Keyspace', {
        extend: 'Ext.data.Model',
        fields: ['keyspace', 'columnFamily'],
        proxy: {
            type: 'rest',
            url: '/keyspaces'
        }
    });

    var keyspaces = Ext.create('Ext.data.Store', {
        model: 'Keyspace',
        autoLoad: true,
        sorters: ['keyspace','columnFamily'],
        groupField: 'keyspace',
    });

    var groupingFeature = Ext.create('Ext.grid.feature.Grouping', {
        groupHeaderTpl: '{name} ({rows.length})'
    });

    Ext.create('Ext.Viewport', {
        title: 'Border Layout',
        layout: 'border',
        fullscreen: true,
        items: [{
            title: 'CQL... Coming soon.',
            region: 'south',
            // position for region
            xtype: 'panel',
            height: 100,
            split: true,
            // enable resizing
            margins: '0 5 5 5'
        },
        {
            title: 'Keyspaces',
            region: 'west',
            xtype: 'gridpanel',
            width: 200,
            margins: '5 0 0 5',
            collapsible: true,
            // make collapsible
            id: 'west-region-container',
            layout: 'fit',
            store: keyspaces,
            features: [groupingFeature],
            columns: [{
                text: 'ColumnFamily',
                flex: 1,
                dataIndex: 'columnFamily'
            }]
        },
        {
            title: 'Data',
            region: 'center',
            // center region is required, no width/height specified
            xtype: 'panel',
            layout: 'fit',
            collapsible: true,
            // make collapsible
            margins: '5 5 0 0'
        }],
        renderTo: Ext.getBody()
    });


});
