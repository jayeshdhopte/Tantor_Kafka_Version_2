import { Info, Radio } from 'lucide-react';
import './Producers.css';

export function Producers() {
  return (
    <section className="producers-tab" aria-labelledby="producers-heading">
      <header className="producers-heading">
        <Radio size={22} aria-hidden="true" />
        <div>
          <h2 id="producers-heading">Producers</h2>
          <p>Producer client observations across this cluster.</p>
        </div>
      </header>
      <div className="producers-unavailable" role="status">
        <Info size={20} aria-hidden="true" />
        <div>
          <strong>Producer information is unavailable</strong>
          <p>Producer discovery is not available in this build. Client IP, client ID and last-seen information will appear once collection is connected.</p>
        </div>
      </div>
    </section>
  );
}
