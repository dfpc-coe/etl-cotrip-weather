import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import { Static, Type, TSchema } from '@sinclair/typebox';

const InputSchema = Type.Object({
    'COTRIP_TOKEN': Type.String({ description: 'API Token for CoTrip' }),
    'DEBUG': Type.Boolean({ description: 'Print GeoJSON results in logs', default: false })
});

export default class Task extends ETL {
    static name = 'etl-cotrip-weather';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                return Type.Object({
                    publicName: Type.String(),
                    direction: Type.String(),
                    nativeId: Type.String(),
                    communicationStatus: Type.String(),
                    marker: Type.String(),
                    routeName: Type.String(),
                    id: Type.String(),
                    lastUpdated: Type.String({ format: 'date-time' }),
                    name: Type.String(),
                });
            }
        } else {
            return Type.Object({});
        }
    }

    async control() {
        const env = await this.env(InputSchema);

        const api = 'https://data.cotrip.org/';
        if (!env.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');

        const stations = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of weather stations`);
            const url = new URL('/api/v1/weatherStations', api);
            url.searchParams.append('apiKey', String(env.COTRIP_TOKEN));
            if (res) url.searchParams.append('offset', res.headers.get('next-offset'));

            res = await fetch(url);

            stations.push(...(await res.json()).features);
        } while (res.headers.has('next-offset') && res.headers.get('next-offset') !== 'None');
        console.log(`ok - fetched ${stations.length} stations`);

        const features = [];
        for (const feature of stations.map((station) => {
            station.id = station.properties.id;

            station.properties = {
                metadata: station.properties
            };

            station.properties.callsign = station.properties.metadata.type;
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

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features
        };

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}
