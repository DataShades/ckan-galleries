"use strict";
ckan.module('asset-map', function ($, _) {
    return {
        map_init: function () {
            // we will need to point to the instance
            var self = this;

            // fetch asset from response
            var asset = self.options.asset;

            // if no asset provided render Canberra map
            if ($.isEmptyObject(asset)) {
                var myLatLng = self.asset_map_center();
                self.assetgeo_empty_map_render(myLatLng);
                return;
            }

            // gets center coordinates
            var myLatLng = self.asset_map_center(asset);

            // Renders map without image
            var map = self.assetgeo_empty_map_render(myLatLng);

            // fits map to circle
            self.assetgeo_fit_to_circle (map, myLatLng);

            // renders image on the map
            var image = self.assetgeo_imagemap_render(map, myLatLng, asset, null);
            setInterval(function () {
                // requests latest asset
                $.ajax({
                    dataType: 'json',
                    url: self.options.host + '/api/3/action/get_last_geo_asset'
                }).done(function (update) {
                    // fetch asset data to JSON format
                    var new_asset = JSON.parse(update.result);
                    // updates latest image if it has changed
                    if (!$.isEmptyObject(new_asset) && new_asset.assetID != asset.assetID) {
                        //sets new asset as current
                        asset = new_asset;
                        // updates infoWindow
                        image = self.assetgeo_imagemap_render(map, self.asset_map_center(asset), asset, image);
                    }
                });
            }, 30000);
        },

        // renders map without any images
        assetgeo_empty_map_render: function (myLatLng) {
            // sets initial map options
            var mapOptions = {
                center: myLatLng,
                zoom: 10,
                disableDefaultUI: true,
                disableDoubleClickZoom: true,
                draggable: false,
                zoomControl: false,
                panControl: false,
                scaleControl: false,
                scrollwheel: false
            };

            // inits map
            return new google.maps.Map(document.getElementById('map-canvas'), mapOptions);
        },

        // fits map to circle
        assetgeo_fit_to_circle: function (map, myLatLng) {
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
        },

        // renders infowindow with image
        assetgeo_imagemap_render: function (map, myLatLng, asset, image) {

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
                image.close();
                map.panTo(myLatLng);
                // fits map to circle
                this.assetgeo_fit_to_circle (map, myLatLng);
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

            // returns default center if no asset provided
            if (!asset) {
                return center;
            }

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
                '<div class="infowindow_image"><img src="' + asset.url + '"/></div>' +
                '<p>' + moment(asset.metadata_created, "YYYY-MM-DD hh:mm:ss").from(this.options.stime) + '</p>' +
                '</div>';
        },
        // adds map to the page
        initialize: function () {
            $.proxyAll(this, /_/);
            this.map_init();
        }
    }
});