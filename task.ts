import ETL, { Event, SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import { Static, Type, TSchema } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';

const InputSchema = Type.Object({
    'COTRIP_TOKEN': Type.String({ description: 'API Token for CoTrip' }),
    'DEBUG': Type.Boolean({ description: 'Print GeoJSON results in logs', default: false })
});

const InputGeometry = Feature.InputFeature.properties.geometry;

const Station = Type.Object({
    geometry: Type.Object({
        type: Type.String(),
        coordinates: Type.Array(Type.Unknown())
    }),
    properties: Type.Object({
        id: Type.Union([Type.String(), Type.Number()]),
        type: Type.Optional(Type.Unknown())
    })
});

const StationResponse = Type.Object({
    features: Type.Array(Station)
});

function toFeature(station: Static<typeof Station>, id: string, geometry: unknown): Static<typeof Feature.InputFeature> | undefined {
    if (!Value.Check(InputGeometry, geometry)) {
        console.error(`not ok - skipping weather station ${id}: unsupported geometry`);
        return undefined;
    }

    return {
        ...station,
        id,
        type: 'Feature',
        properties: {
            metadata: station.properties,
            callsign: typeof station.properties.type === 'string' ? station.properties.type : undefined,
            type: 'a-f-G'
        },
        geometry
    };
}

export default class Task extends ETL {
    static name = 'etl-cotrip-weather';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) return InputSchema;

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

        return Type.Object({});
    }

    async control() {
        const env = await this.env(InputSchema);

        const api = 'https://data.cotrip.org/';
        if (!env.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');

        const stations: Array<Static<typeof Station>> = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of weather stations`);
            const url = new URL('/api/v1/weatherStations', api);
            url.searchParams.append('apiKey', env.COTRIP_TOKEN);
            if (res) {
                const nextOffset = res.headers.get('next-offset');
                if (nextOffset) url.searchParams.append('offset', nextOffset);
            }

            res = await fetch(url);

            const body: unknown = await res.json();
            if (!Value.Check(StationResponse, body)) throw new Error(`Unexpected CoTrip response: ${res.status}`);

            stations.push(...body.features);
        } while (res.headers.has('next-offset') && res.headers.get('next-offset') !== 'None');
        console.log(`ok - fetched ${stations.length} stations`);

        const features: Array<Static<typeof Feature.InputFeature>> = [];
        for (const station of stations) {
            const id = String(station.properties.id);

            if (station.geometry.type.startsWith('Multi')) {
                const type = station.geometry.type.replace('Multi', '');

                for (const [i, coordinates] of station.geometry.coordinates.entries()) {
                    const feat = toFeature(structuredClone(station), `${id}-${i}`, { type, coordinates });
                    if (feat) features.push(feat);
                }
            } else {
                const feat = toFeature(station, id, station.geometry);
                if (feat) features.push(feat);
            }
        }

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features
        };

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url), event);
}
