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
      html = $('<button class="btn btn-danger">');
      html.on('click', _onRemove);
      html.text('Remove');
      this.el.popover({title: 'Actions', html: true,
                       content: html, placement: 'left'});
      this.el.popover('show');


      this.sandbox.publish('asset_context_clicked', this.el);
    },

    _onPopoverClicked: function(button) {
      if (button != this.el) {
        this.el.popover('destroy');
      }
    },

    _onRemove: function (event) {

      var self = this
        $.ajax({
          url:  'URL FOR ACTION',
          method:'POST',
          data:{
            action:'ACTION',
            assets:'ASSET ID',
            res_id:'RES ID'
          },
          success: function (data) {
            window.location.reload();
          },
          error: function (error, data) {
            'SELF POPUP ERROR TEXT'
          }
        });
    }




  }
});