import fs from 'fs';
import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { FeatureCollection, Feature } from 'geojson';
import { Type, TSchema } from '@sinclair/typebox';

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
                'COTRIP_TOKEN': Type.String({ description: 'API Token for CoTrip' }),
                'DEBUG': Type.Boolean({ description: 'Print GeoJSON results in logs', default: false })
            });
        } else {
            return Type.Object({});
        }
    }

    async control() {
        const layer = await this.fetchLayer();

        const api = 'https://data.cotrip.org/';
        if (!layer.environment.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');
        const token = layer.environment.COTRIP_TOKEN;

        const stations = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of weather stations`);
            const url = new URL('/api/v1/weatherStations', api);
            url.searchParams.append('apiKey', String(token));
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

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: features
        };

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}
