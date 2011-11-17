Ext.require(['Ext.data.*', 'Ext.grid.*']);

Ext.Ajax.defaultHeaders = {
    'Accept': 'application/json'
};

Ext.onReady(function() {
    Ext.define('Keyspace', {
        extend: 'Ext.data.Model',
        fields: ['name'],
        hasMany: {
            model: 'ColumnFamily'
        },
        proxy: {
            type: 'rest',
            url: '/keyspaces/'
        }
    });

    Ext.define('ColumnFamily', {
        extend: 'Ext.data.Model',
        fields: ['name'],
        belongsTo: 'Keyspace'
    });

    var keyspaces = Ext.create('Ext.data.Store', {
        model: 'Keyspace',
        autoLoad: true
    });

    var grid = Ext.create('Ext.grid.Panel', {
        collapsible: true,
        iconCls: 'icon-grid',
        frame: true,
        store: keyspaces,
        title: 'keyspaces',
        columns: [{
            text: 'Name',
            flex: 1,
            dataIndex: 'name'
        }]
    });

    Ext.create('Ext.Viewport', {
        title: 'Border Layout',
        layout: 'border',
        fullscreen: true,
        items: [{
            title: 'South Region is resizable',
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
            columns: [{
                text: 'Name',
                flex: 1,
                dataIndex: 'name'
            }]
        },
        {
            title: 'Center Region',
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
