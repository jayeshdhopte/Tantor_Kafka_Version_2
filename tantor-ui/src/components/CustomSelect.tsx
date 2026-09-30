import { useState } from 'react';
import { ChevronDown } from 'lucide-react';
import { AnchoredMenu } from './AnchoredMenu';
import './CustomSelect.css';

interface Option {
  value: string;
  label: string;
}

interface CustomSelectProps {
  value: string;
  onChange: (value: string) => void;
  options: Option[];
  width?: string;
  placeholder?: string;
  variant?: 'default' | 'audit';
}

export function CustomSelect({ value, onChange, options, width = '209px', placeholder, variant = 'default' }: CustomSelectProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [anchor, setAnchor] = useState<HTMLDivElement | null>(null);

  const selectedOption = options.find(opt => opt.value === value);

  return (
    <div className={`custom-select-container ${variant === 'audit' ? 'audit-style-select' : ''}`} ref={setAnchor} style={{ width }}>
      <button
        type="button"
        className={`custom-select-trigger ${isOpen ? 'open' : ''}`}
        onClick={() => setIsOpen(!isOpen)}
        aria-haspopup="listbox"
        aria-expanded={isOpen}
        title={selectedOption?.label || placeholder || 'Select option'}
      >
        <span className="custom-select-value">
          {selectedOption ? selectedOption.label : placeholder || 'Select option'}
        </span>
        <ChevronDown size={18} className="custom-select-chevron" />
      </button>

      {isOpen && anchor && (
        <AnchoredMenu
          anchor={anchor}
          className={`custom-select-options-wrapper ${variant === 'audit' ? 'audit-style-select-options' : ''}`}
          onClose={() => setIsOpen(false)}
          align="start"
          matchAnchorWidth
        >
          <div className="app-custom-select-options" role="listbox">
            {options.map(option => (
              <button
                key={option.value}
                type="button"
                className={`app-custom-select-option ${option.value === value ? 'selected' : ''}`}
                role="option"
                aria-selected={option.value === value}
                title={option.label}
                onClick={() => {
                  onChange(option.value);
                  setIsOpen(false);
                }}
              >
                {option.label}
              </button>
            ))}
          </div>
        </AnchoredMenu>
      )}
    </div>
  );
}
