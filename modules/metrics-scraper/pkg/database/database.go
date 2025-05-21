// Copyright 2017 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package database

import (
	"database/sql"
	"fmt"
	"time"

	"k8s.io/dashboard/metrics-scraper/pkg/dcgm"
	"k8s.io/klog/v2"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"k8s.io/dashboard/metrics-scraper/pkg/args"
)

/*
CreateDatabase creates tables for node and pod metrics
*/
func CreateDatabase(db *sql.DB) error {
	sqlStmt := `
	create table if not exists nodes (uid text, name text, cpu text, memory text, storage text, time datetime);
	create table if not exists pods (uid text, name text, namespace text, container text, cpu text, memory text, storage text, time datetime);
	create table if not exists dcgm_metrics (
		uuid text,
		name text,
		namespace text,
		pod text,
		container text,
		gpu text,
		pci_bus_id text,
		device text,
		hostname text,
		driver_version text,
		value integer,
		time datetime
	);
	`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		return err
	}

	return nil
}

/*
UpdateDatabase updates nodeMetrics and podMetrics with scraped data
*/
func UpdateDatabase(db *sql.DB, nodeMetrics *v1beta1.NodeMetricsList, podMetrics *v1beta1.PodMetricsList, dcgmMetrics *dcgm.DCGMMetricList) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("insert into nodes(uid, name, cpu, memory, storage, time) values(?, ?, ?, ?, ?, datetime('now'))")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, v := range nodeMetrics.Items {
		_, err = stmt.Exec(v.UID, v.Name, v.Usage.Cpu().MilliValue(), v.Usage.Memory().MilliValue()/1000, v.Usage.StorageEphemeral().MilliValue()/1000)
		if err != nil {
			return err
		}
	}

	stmt, err = tx.Prepare("insert into pods(uid, name, namespace, container, cpu, memory, storage, time) values(?, ?, ?, ?, ?, ?, ?, datetime('now'))")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, v := range podMetrics.Items {
		for _, u := range v.Containers {
			_, err = stmt.Exec(v.UID, v.Name, v.Namespace, u.Name, u.Usage.Cpu().MilliValue(), u.Usage.Memory().MilliValue()/1000, u.Usage.StorageEphemeral().MilliValue()/1000)
			if err != nil {
				return err
			}
		}
	}

	stmt, err = tx.Prepare("insert into dcgm_metrics(uuid, name, namespace, pod, container, gpu, pci_bus_id, device, hostname, driver_version, value, time) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, v := range dcgmMetrics.Metrics {
		_, err = stmt.Exec(v.UUID, v.Name, v.Namespace, v.Pod, v.Container, v.GPU, v.PCIBusID, v.Device, v.Hostname, v.DriverVer, v.Value)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()

	if err != nil {
		rberr := tx.Rollback()
		if rberr != nil {
			return rberr
		}
		return err
	}

	return nil
}

/*
CullDatabase deletes rows from nodes and pods based on a time window.
*/
func CullDatabase(db *sql.DB, window time.Duration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	windowStr := fmt.Sprintf("-%.0f seconds", window.Seconds())

	nodestmt, err := tx.Prepare("delete from nodes where time <= datetime('now', ?);")
	if err != nil {
		return err
	}

	defer nodestmt.Close()
	res, err := nodestmt.Exec(windowStr)
	if err != nil {
		return err
	}

	affected, _ := res.RowsAffected()
	klog.V(args.LogLevelDebug).Infof("Cleaning up nodes: %d rows removed", affected)

	podstmt, err := tx.Prepare("delete from pods where time <= datetime('now', ?);")
	if err != nil {
		return err
	}

	defer podstmt.Close()
	res, err = podstmt.Exec(windowStr)
	if err != nil {
		return err
	}

	affected, _ = res.RowsAffected()
	klog.V(args.LogLevelDebug).Infof("Cleaning up pods: %d rows removed", affected)
	err = tx.Commit()

	if err != nil {
		rberr := tx.Rollback()
		if rberr != nil {
			return rberr
		}
		return err
	}

	return nil
}

// UpdateDCGMMetrics updates the database with DCGM metrics
// func UpdateDCGMMetrics(db *sql.DB, metrics dcgm.DCGMMetricDCGMMetric) error {
// 	tx, err := db.Begin()
// 	if err != nil {
// 		return err
// 	}

// 	stmt, err := tx.Prepare(`
// 		insert into dcgm_metrics(
// 			uid, name, namespace, container, gpu_id,
// 			gpu_utilization, memory_utilization, memory_used, memory_total,
// 			power_usage, temperature, time
// 		) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
// 	`)
// 	if err != nil {
// 		return err
// 	}
// 	defer stmt.Close()

// 	for _, metric := range metrics {
// 		_, err = stmt.Exec(
// 			metric.UID,
// 			metric.Name,
// 			metric.Namespace,
// 			metric.Container,
// 			metric.GPUId,
// 			metric.GPUUtilization,
// 			metric.MemoryUtilization,
// 			metric.MemoryUsed,
// 			metric.MemoryTotal,
// 			metric.PowerUsage,
// 			metric.Temperature,
// 		)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	err = tx.Commit()
// 	if err != nil {
// 		rberr := tx.Rollback()
// 		if rberr != nil {
// 			return rberr
// 		}
// 		return err
// 	}

// 	return nil
// }
