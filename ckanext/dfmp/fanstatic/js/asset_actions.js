"use strict";
ckan.module('asset-actions', function ($, _){
  return{
    initialize: function (){
      $.proxyAll(this, /_on/);
      $('.remove-action').on('click', this._onRemove)
      $('.hide-action').on('click', this._onHide)

    },

    _onRemove: function(e){
      console.log(this.options);
    },

    _onHide: function(e){
      console.log(this.options);
    },

  };
});