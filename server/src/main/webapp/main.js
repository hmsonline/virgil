Ext.require(['Ext.data.*', 'Ext.grid.*']);

Ext.Ajax.defaultHeaders = {
    'Accept': 'application/json'
};

Ext.onReady(function() {

	// ================================================================================
	// Column Family
	// ================================================================================
    Ext.define('ColumnFamily', {
        extend: 'Ext.data.Model',
        fields: ['row', 'column', 'value'],
        proxy: {
            type: 'ajax',
            url: '/keyspaces/datastore/pets/'
        }
    });

    var columnFamily = Ext.create('Ext.data.Store', {
        model: 'ColumnFamily',
        autoLoad: true,
        sorters: ['row', 'column'],
        groupField: 'row',
    });

    var rowGroupingFeature = Ext.create('Ext.grid.feature.Grouping', {
        groupHeaderTpl: '{name} ({rows.length})'
    });

    var columnFamilyGrid = Ext.create('Ext.grid.Panel', {
        title: 'Column Family',
        region: 'center',
        xtype: 'gridpanel',
        margins: '5 5 0 0',
        collapsible: true,
        // make collapsible
        layout: 'fit',
        store: columnFamily,
        sm: Ext.create('Ext.selection.RowModel').setSelectionMode('SINGLE'),
        features: [rowGroupingFeature],
        columns: [{
            text: 'Column',
            flex: 1,
            dataIndex: 'column'
        },
        {
            text: 'Value',
			flex: 1,
            dataIndex: 'value',
            renderer: function(value){
                var display = '<p style="white-space:normal">' + value + '</p>';
                return display;
            }
        }]
    });

	
	// ================================================================================
	// Keyspace
	// ================================================================================
    Ext.define('Keyspace', {
        extend: 'Ext.data.Model',
        fields: ['keyspace', 'columnFamily'],
        proxy: {
            type: 'rest',
            url: '/virgil/data/'
        }
    });

    var keyspaces = Ext.create('Ext.data.Store', {
        model: 'Keyspace',
        autoLoad: true,
        sorters: ['keyspace', 'columnFamily'],
        groupField: 'keyspace',
    });

    var keyspaceGroupingFeature = Ext.create('Ext.grid.feature.Grouping', {
        groupHeaderTpl: '{name} ({rows.length})'
    });

    var keyspacesGrid = Ext.create('Ext.grid.Panel', {
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
        sm: Ext.create('Ext.selection.RowModel').setSelectionMode('SINGLE'),
        features: [keyspaceGroupingFeature],
        columns: [{
            text: 'ColumnFamily',
            flex: 1,
            dataIndex: 'columnFamily'
        }],
        listeners: {
            itemclick: function(vie, record) {
				cfUrl = "/virgil/data/" + record.raw["keyspace"] + "/" + record.raw["columnFamily"] + "/"
                console.info("Fetching [" + cfUrl + "]");
				ColumnFamily.proxy.url = cfUrl;
				columnFamily.load();
            }
        }
    });

	// ================================================================================
	// Main Viewport
	// ================================================================================

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
        keyspacesGrid,
        columnFamilyGrid],
        renderTo: Ext.getBody()
    });

});
