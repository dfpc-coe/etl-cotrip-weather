import fs from 'fs';
import ETL from '@tak-ps/etl';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(fs.readFileSync(dotfile)));
    console.log('ok - .env file loaded');
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static async schema(type = 'input') {
        if (type === 'input') {
            return {
                type: 'object',
                required: ['COTRIP_TOKEN'],
                properties: {
                    'COTRIP_TOKEN': {
                        type: 'string',
                        description: 'API Token for CoTrip'
                    },
                    'DEBUG': {
                        type: 'boolean',
                        default: false,
                        description: 'Print GeoJSON Features in logs'
                    }
                }
            };
        } else {
            return {
                type: 'object',
                required: [],
                properties: {
                }
            };
        }
    }

    async control() {
        const layer = await this.layer();

        const api = 'https://data.cotrip.org/';
        if (!layer.environment.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');
        const token = layer.environment.COTRIP_TOKEN;

        const stations = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of weather stations`);
            const url = new URL('/api/v1/weatherStations', api);
            url.searchParams.append('apiKey', token);
            if (res) url.searchParams.append('offset', res.headers.get('next-offset'));

            res = await fetch(url);

            stations.push(...(await res.json()).features);
        } while (res.headers.has('next-offset') && res.headers.get('next-offset') !== 'None');
        console.log(`ok - fetched ${stations.length} stations`);

        const features = [];
        for (const feature of stations.map((station) => {
            station.id = station.properties.id;
            station.properties.callsign = station.properties.type;
            delete station.properties.type;
            delete station.properties.status;
            station.properties.type = 'a-f-G';
            return station;
        })) {
            if (feature.geometry.type.startsWith('Multi')) {
                const feat = JSON.stringify(feature);
                const type = feature.geometry.type.replace('Multi', '');

                let i = 0;
                for (const coordinates of feature.geometry.coordinates) {
                    const new_feat = JSON.parse(feat);
                    new_feat.geometry = { type, coordinates };
                    new_feat.id = new_feat.id + '-' + i;
                    features.push(new_feat);
                    ++i;
                }
            } else {
                features.push(feature);
            }
        }

        const fc = {
            type: 'FeatureCollection',
            features: features
        };

        await this.submit(fc);
    }
}

export async function handler(event = {}) {
    if (event.type === 'schema:input') {
        return await Task.schema('input');
    } else if (event.type === 'schema:output') {
        return await Task.schema('output');
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();
