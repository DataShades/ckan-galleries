"use strict";

ckan.module('dfmp-removable-asset', function ($, _) {
  return {
    initialize: function(){
      $.proxyAll(this, /_on/);
      
      // this.el.popover({title: 'Actions', html: true,
      //                  content: 'Loading...', placement: 'left'});


      this.el.on('contextmenu', this._onContext);
      this.el.on('blur', this._onBlur);
 

      this.sandbox.subscribe('asset_context_clicked', this._onPopoverClicked);
    },

    teardown: function() {
      this.sandbox.unsubscribe('asset_context_clicked', this._onPopoverClicked);
    },

    _onBlur: function (event) {
      this.el.popover('destroy');
    }, 

    _onContext: function(event) {
      event.preventDefault();
      this.el.popover('destroy');
      html = $('<div>');
      removeButton = $('<button class="btn btn-danger">');
      removeButton.text('Remove');
      removeButton.on('click', this._onRemove);
      html.append(removeButton)
      this.el.popover({title: 'Actions', html: true,
                       content: html, placement: 'left', width:'100px'});
      this.el.popover('show');


      this.sandbox.publish('asset_context_clicked', this.el);
    },

    _onPopoverClicked: function(button) {
      if (button != this.el) {
        this.el.popover('destroy');
      }
    },

    _onRemove: function (event) {
      this._onAction({
        data:{action:'delete', message:'Removed'}
      })


    },

    _onAction: function(e){
      var self = this;
      console.log(this);
      var values = this.options.asset.slice(1)
      console.log(values);

      $('.asset-actions', self.el).html('In progress...');
      
      $.ajax({
        url:this.options.url,
        method:'POST',
        data:{
          action:e.data.action,
          assets:values,
          res_id:this.options.resource,
          without_forbidding:true,
        },
        success: function (data) {
          window.location.reload();
        },
        error: function (error, data) {
          $('.asset-actions', self.el).html(error.status + ' ' + error.statusText);
        }
      });
    },




  }
});