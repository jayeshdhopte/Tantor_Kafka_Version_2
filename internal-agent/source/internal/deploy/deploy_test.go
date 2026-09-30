package deploy

import (
	"reflect"
	"testing"
)

func TestPrerequisitePorts(t *testing.T) {
	got, err := prerequisitePorts("9092,2181,2888,3888,2181")
	if err != nil {
		t.Fatalf("prerequisitePorts() error = %v", err)
	}
	want := []string{"9092", "2181", "2888", "3888"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("prerequisitePorts() = %v, want %v", got, want)
	}
}

func TestPrerequisitePortsUsesKafkaDefaults(t *testing.T) {
	got, err := prerequisitePorts("")
	if err != nil {
		t.Fatalf("prerequisitePorts() error = %v", err)
	}
	want := []string{"9092", "9093", "7071"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("prerequisitePorts() = %v, want %v", got, want)
	}
}

func TestPrerequisitePortsRejectsInjectionAndInvalidRange(t *testing.T) {
	for _, value := range []string{"9092;id", "9092 && id", "9092$(id)", "9092\nwhoami", "65536"} {
		if _, err := prerequisitePorts(value); err == nil {
			t.Errorf("prerequisitePorts(%q) unexpectedly succeeded", value)
		}
	}
}
