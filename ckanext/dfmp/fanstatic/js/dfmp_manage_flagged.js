"use strict";

ckan.module('dfmp-manage-flagged', function ($, _) {
  return {
    initialize: function (){
      $.proxyAll(this, /_on/);
      this.el.on(
        'click', this._onClick);

    },

    _onClick: function(e){
      var self = this;
      this.el.find('i').addClass('in-progress');

      $.ajax({
        url:this.options.url,
        method:'POST',
        data:{
          action:this.options.action,
          assets:this.options.asset.slice(1),
          res_id:this.options.res
        },
        success: function (data) {
          window.location.reload();
        },
        error: function (error, data) {
          console.log(error.status + ' ' + error.statusText);
        }
      });
    },

  }
})