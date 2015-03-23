"use strict";
ckan.module('asset-actions', function ($, _){
  return{
    initialize: function (){
      $.proxyAll(this, /_on/);
      $('.remove-action', this.el).on('click', {action:'delete', message:'Removed'}, this._onAction)
      $('.hide-action', this.el).on('click', {action:'hide', message:'Hidden'}, this._onAction)
      $('.solr-remove-action', this.el).on('click', {action:'solr-delete', message:'Removed'}, this._onAction)
      $('.unhide-action', this.el).on('click', {action:'unhide', message:'Shown'}, this._onAction)

    },

    _onAction: function(e){
      var self = this;

      var values = [];
      var checked = $(':checked', this.el);
      for (var i = 0; i < checked.length; values.push(checked[i].value.slice(1)), i++);
      console.log(values)
      if (!values.length) return;
      $('.asset-actions', self.el).html('In progress...');
      
      $.ajax({
        url:this.options.url,
        method:'POST',
        data:{
          action:e.data.action,
          assets:values.join(' '),
          res_id:this.options.id
        },
        success: function (data) {
          $('.asset-actions', self.el).html(e.data.message);
        },
        error: function (error, data) {
          $('.asset-actions', self.el).html(error.status + ' ' + error.statusText);
        }
      });
    },

  };
});