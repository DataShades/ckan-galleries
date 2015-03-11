"use strict";

ckan.module('asset_last_items', function ($, _) {
  return {
    initialize: function () {
      console.log("I've been initialized for element: ", this.el);
      console.log(this.options.res);
    }
  };
});