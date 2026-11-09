import DataUnavailable from '../components/DataUnavailable'

// AER economic benchmarking (RIN) data is not ingested in this build; the API returns 503 with the reason.
export default function PeerComparison() {
  return <DataUnavailable title="DNSP Peer Comparison" endpoint="/api/benchmarking/peers" />
}
