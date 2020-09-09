PLUGIN_VERSION=1.0.0
PLUGIN_ID=splunk

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip code-env parameter-sets python-connectors python-exporters plugin.json
