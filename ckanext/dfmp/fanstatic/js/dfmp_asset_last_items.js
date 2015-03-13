"use strict";

ckan.module('asset_last_items', function ($, _) {
  return {
    initialize: function () {
      
      $.proxyAll(this, /_on/);
      this.el.popover({title: 'Last activities', html: true,
                       content: 'Loading...', placement: 'left'});

      

      this.el.on('click', this._onClick);

      this.sandbox.subscribe('dataset_popover_clicked',
                             this._onPopoverClicked);
    },

    teardown: function() {
      this.sandbox.unsubscribe('dataset_popover_clicked',
                               this._onPopoverClicked);
    },
    _snippetReceived: false,

    _onClick: function(event) {
        if (!this._snippetReceived) {
          var self = this;

          $.ajax({
             url: this.options.host + '/api/3/action/datastore_search',
             data: {
              id: this.options.res,
              limit: 10,
              sort:'lastModified desc'
            },
             success: self._onReceiveSnippet
          })
            
            this._snippetReceived = true;
        }

        this.sandbox.publish('dataset_popover_clicked', this.el);
    },

    _onPopoverClicked: function(button) {
      if (button != this.el) {
        this.el.popover('hide');
      }
    },

    _onReceiveSnippet: function(html) {
      var records = html.result.records;
      var ammount = records.length;

      var chtml = '<table class="table table-striped table-bordered table-condensed center-aligned" >';
      for (var i = 0; i < ammount; i++){
        var time =records[i]['lastModified'].slice(0, 19);
        chtml += '<tr><td>' + records[i]['name'] + '</td><td>' +  moment(time, "YYYY-MM-DD hh:mm:ss").from(this.options.stime)  + '</td></tr>';
       
      }
      chtml += '</table>';

      this.el.popover('destroy');
      this.el.popover({title: 'Last activities', html: true,
                       content: chtml, placement: 'left'});
      this.el.popover('show');
    },

  };
});