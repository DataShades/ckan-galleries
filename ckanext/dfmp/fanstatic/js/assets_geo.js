"use strict";
ckan.module('asset-map', function ($, _) {
    return {
        map_init: function () {
            // we will need to point to the instance
            var self = this;

            // we need to get the latest image first
            $.ajax({
                dataType: 'json',
                url: self.options.host + '/api/3/action/get_last_geo_asset'
            }).done(function (response) {

                // fetch asset from response
                var asset = response.result;

                // gets center coordinates
                var myLatLng = self.asset_map_center(asset);

                // sets initial map options
                var mapOptions = {
                    center: myLatLng,
                    zoom: 10,
                    disableDefaultUI: true,
                    draggable: false,
                    zoomControl: false,
                    panControl: false,
                    scaleControl: false,
                    scrollwheel: false
                };

                // inits map
                var map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);

                // Creates circle with provided settings
                var circleOptions = {
                    center: myLatLng,
                    fillOpacity: 0,
                    strokeOpacity: 0,
                    map: map,
                    radius: 2000
                };
                var myCircle = new google.maps.Circle(circleOptions);

                // makes map to fit the circle
                map.fitBounds(myCircle.getBounds());

                // renders image on the map
                var image = self.asset_map_render(map, myLatLng, asset, null);
                setInterval(function () {
                    // requests latest asset
                    $.ajax({
                        dataType: 'json',
                        url: self.options.host + '/api/3/action/get_last_geo_asset'
                    }).done(function (update) {
                        // fetch asset data to JSON format
                        var new_asset = update.result;
                        // updates latest image if it has changed
                        if (new_asset.assetID != asset.assetID) {
                            //sets new asset as current
                            asset = new_asset;
                            // updates infoWindow
                            image = self.asset_map_render(map, self.asset_map_center(asset), asset, image);
                        }
                    });
                }, 30000);
            })
        },

        // renders infowindow with image
        asset_map_render: function (map, myLatLng, asset, image) {

            var description_truncate = function () {
                var content = document.querySelector('.gm-style-iw');
                try {
                    content.parentNode.removeChild(content.nextElementSibling);
                }
                catch (TypeError) {
                }
                content.style.setProperty('width', 'auto', 'important');
                content.style.setProperty('right', content.style.left, 'important');
                content.style.setProperty('text-align', 'center', 'important');
                // truncates long descriptions
                $('.infowindow_desc').trunk8({
                    lines: 2
                });
            };

            // renders image on the map if it is not rendered yet
            if (!image) {
                image = this.asset_infowindow_add_dom (map, myLatLng, asset);
            }
            // changes the content and position of asset
            else {
                map.panTo(myLatLng);
                image.close();
                image = this.asset_infowindow_add_dom (map, myLatLng, asset);
            }

            // truncates description
            google.maps.event.addListener(image, 'domready', description_truncate);

            return image;
        },

        // adds infoWindow to the DOM
        asset_infowindow_add_dom: function (map, myLatLng, asset) {
             var image = new google.maps.InfoWindow({
                 position: myLatLng,
                 map: map,
                 content: this.asset_infowindow_content(asset),
                 // image width
                 maxWidth: 160,
                 disableAutoPan: false
             });

            return image;
        },

        // returns center of map
        asset_map_center: function (asset) {
            // DEFAULT COORDINATES
            var center = {
                lat: -35.31397979,
                lng: 149.12978252799996
            };

            // calculates the center if Polygon is provided.
            if (asset.spatial.type == 'Polygon') {
                var bounds = new google.maps.LatLngBounds(),
                    polygonCoords = [];

                // collects all polygon coordinates
                $.each(asset.spatial.coordinates[0], function (key, val) {
                    bounds.extend(new google.maps.LatLng(val[1], val[0]));
                });

                // sets center coordinate
                center = {
                    lat: bounds.getCenter().lat(),
                    lng: bounds.getCenter().lng()
                };
            }
            // or gets point coordinates from asset object
            else {
                center = {
                    lat: asset.spatial.coordinates[1],
                    lng: asset.spatial.coordinates[0]
                };
            }

            return new google.maps.LatLng(center.lat, center.lng);
        },

        // return content for infoWindow
        asset_infowindow_content: function (asset) {
            return '<div id="latest_asset_infowindow">' +
                '<h4>' + asset.name + '</h4>' +
                '<p class="infowindow_desc">' + asset.notes + '</p>' +
                '<img src="' + asset.url + '"/>' +
                '<p>' + new Date(asset.metadata_created).toTwitterRelativeTime() + '</p>' +
                '</div>';
        },
        // adds map to the page
        initialize: function () {
            $.proxyAll(this, /_/);
            this.map_init();
        }
    }
});