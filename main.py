# -*- coding: utf-8 -*-

import os
from google.cloud import documentai
from google.cloud import storage
import json
import time

from flask import Flask, request

app = Flask(__name__)

# Globaler Cache für verarbeitete Events (in Produktionsumgebung sollte Redis/Memcache verwendet werden)
processed_events = {}

@app.route("/", methods=["POST"])
def http_entrypoint():
    # Cloud Functions (2nd gen) und Cloud Run schicken das Event als JSON im Body
    event = request.get_json(force=True)
    
    # Event-ID für Deduplizierung extrahieren
    event_id = None
    if 'ce-eventid' in request.headers:
        event_id = request.headers['ce-eventid']
    elif 'eventId' in event:
        event_id = event['eventId']
    
    # Kontext ist optional, kann leer bleiben
    context = {}
    process_document_from_gcs(event, context, event_id)
    return ("", 204)

def process_document_from_gcs(event, context, event_id=None):
    """
    Diese Cloud Function wird durch einen Datei-Upload in einen Google Cloud Storage
    Bucket ausgelöst. Sie analysiert das Dokument mit Document AI und speichert
    den extrahierten Text in einem anderen Bucket.

    Args:
        event (dict): Das Event-Payload, das von Google Cloud bereitgestellt wird.
                      Es enthält Details zur hochgeladenen Datei.
        context (google.cloud.functions.Context): Metadaten zum Event.
    """
    # --- 1. Konfiguration aus Umgebungsvariablen laden ---
    # Diese Variablen müssen in der Google Cloud Function Konfiguration gesetzt werden.
    # Dies ist sicherer und flexibler als das Eintragen der Werte direkt in den Code.
    try:
        project_id = os.environ["GCP_PROJECT"]
        location = os.environ["PROCESSOR_LOCATION"]  # z.B. 'eu' oder 'us'
        processor_id = os.environ["PROCESSOR_ID"]
        output_bucket_name = os.environ["OUTPUT_BUCKET"]
    except KeyError as e:
        print(f"Fehler: Die Umgebungsvariable {e} ist nicht gesetzt!")
        # Beendet die Funktion, da die Konfiguration unvollständig ist.
        return

    # --- 2. Informationen zur Trigger-Datei aus dem Event auslesen ---
    input_bucket_name = event["bucket"]
    file_name = event["name"]
    content_type = event.get("contentType", "")

    print(f"✅ Datei erkannt: gs://{input_bucket_name}/{file_name}")
    
    # Prüfen, ob es sich um den Output-Bucket handelt - wenn ja, ignorieren
    if input_bucket_name == output_bucket_name:
        print(f"⚠️ Datei stammt aus dem Output-Bucket ({output_bucket_name}). Verarbeitung wird übersprungen.")
        return

    # Prüfen, ob die Datei bereits verarbeitet wurde (hat .txt Endung oder enthält "_verarbeitet")
    if file_name.endswith('.txt') or '_verarbeitet' in file_name:
        print(f"⚠️ Datei '{file_name}' scheint bereits verarbeitet zu sein. Verarbeitung wird übersprungen.")
        return

    # Sicherstellen, dass nur PDF-Dateien verarbeitet werden, um Fehler zu vermeiden.
    if not content_type == "application/pdf":
        print(f"⚠️ Datei '{file_name}' ist keine PDF-Datei ({content_type}). Verarbeitung wird übersprungen.")
        return

    # --- 3. Document AI und Storage Clients initialisieren ---
    storage_client = storage.Client()
    
    # **WICHTIG: Zuerst prüfen, ob die Ausgabedatei bereits existiert**
    # Das ist die dauerhafte Prüfung, die unabhängig vom Event-Cache funktioniert
    output_filename = f"{os.path.splitext(file_name)[0]}.txt"
    output_bucket = storage_client.bucket(output_bucket_name)
    output_blob = output_bucket.blob(output_filename)
    
    if output_blob.exists():
        print(f"⚠️ Ausgabedatei '{output_filename}' existiert bereits. Verarbeitung wird übersprungen.")
        return

    # Event-Deduplizierung - verhindert mehrfache Verarbeitung desselben Events (nur für kurze Zeit)
    if event_id:
        cache_key = f"{event_id}_{input_bucket_name}_{file_name}"
        current_time = time.time()
        
        # Prüfen, ob dieses Event bereits verarbeitet wurde (Cache für 10 Minuten)
        if cache_key in processed_events:
            if current_time - processed_events[cache_key] < 600:  # 10 Minuten
                print(f"⚠️ Event {event_id} bereits verarbeitet. Überspringe.")
                return
        
        # Event als verarbeitet markieren
        processed_events[cache_key] = current_time
        
        # Alte Einträge aus dem Cache entfernen (älter als 10 Minuten)
        to_remove = [k for k, v in processed_events.items() if current_time - v > 600]
        for k in to_remove:
            del processed_events[k]
        
        print(f"🔄 Verarbeite Event {event_id}")

    # Prüfen, ob es sich um den Output-Bucket handelt - wenn ja, ignorieren
    if input_bucket_name == output_bucket_name:
        print(f"⚠️ Datei stammt aus dem Output-Bucket ({output_bucket_name}). Verarbeitung wird übersprungen.")
        return

    # Prüfen, ob die Datei bereits verarbeitet wurde (hat .txt Endung oder enthält "_verarbeitet")
    if file_name.endswith('.txt') or '_verarbeitet' in file_name:
        print(f"⚠️ Datei '{file_name}' scheint bereits verarbeitet zu sein. Verarbeitung wird übersprungen.")
        return

    # Sicherstellen, dass nur PDF-Dateien verarbeitet werden, um Fehler zu vermeiden.
    if not content_type == "application/pdf":
        print(f"⚠️ Datei '{file_name}' ist keine PDF-Datei ({content_type}). Verarbeitung wird übersprungen.")
        return

    # --- 3. Document AI und Storage Clients initialisieren ---
    # Es wird empfohlen, Clients innerhalb der Funktion zu initialisieren.
    storage_client = storage.Client()
    # WICHTIG: Den API-Endpunkt explizit basierend auf der Location setzen.
    # Für EU-Region muss der Endpunkt korrekt sein
    if location == "eu":
        api_endpoint = "eu-documentai.googleapis.com"
    elif location == "us":
        api_endpoint = "us-documentai.googleapis.com"
    else:
        api_endpoint = f"{location}-documentai.googleapis.com"
    
    print(f"🔗 Verwende API-Endpunkt: {api_endpoint}")
    opts = {"api_endpoint": api_endpoint}
    docai_client = documentai.DocumentProcessorServiceClient(client_options=opts)

    # Den vollständigen Pfad zum Document AI Prozessor zusammenbauen.
    processor_path = docai_client.processor_path(project_id, location, processor_id)
    print(f"📍 Prozessor-Pfad: {processor_path}")

    # --- 4. Dokument aus dem Storage laden und verarbeiten ---
    print(f"⚙️ Verarbeite Dokument mit Prozessor: {processor_id}")
    try:
        # Die Datei aus dem Input-Bucket als Bytes herunterladen.
        input_bucket = storage_client.bucket(input_bucket_name)
        input_blob = input_bucket.blob(file_name)
        image_content = input_blob.download_as_bytes()

        # Das Dokument für die API-Anfrage vorbereiten.
        raw_document = documentai.RawDocument(
            content=image_content,
            mime_type=content_type,
        )

        # Die Anfrage an die Document AI API senden.
        request = documentai.ProcessRequest(
            name=processor_path,
            raw_document=raw_document
        )
        result = docai_client.process_document(request=request)
        document = result.document
        print("📄 Dokument erfolgreich verarbeitet.")

    except Exception as e:
        print(f"❌ Fehler bei der Document AI Verarbeitung: {e}")
        return

    # --- 5. Ergebnis in den Output-Bucket speichern ---
    # Den extrahierten Text aus dem Ergebnisobjekt holen.
    # Für Formular-Prozessoren könnten Sie hier `document.entities` durchlaufen.
    extracted_text = document.text
    print(f"🔍 {len(extracted_text)} Zeichen extrahiert.")

    # Den Dateinamen für die Ausgabedatei festlegen (z.B. original.pdf -> original.txt).
    output_filename = f"{os.path.splitext(file_name)[0]}.txt"
    
    try:
        output_bucket = storage_client.bucket(output_bucket_name)
        output_blob = output_bucket.blob(output_filename)
        
        # Den extrahierten Text in die neue Datei hochladen.
        output_blob.upload_from_string(extracted_text, content_type="text/plain; charset=utf-8")
        
        print(f"🎉 Ergebnis erfolgreich in gs://{output_bucket_name}/{output_filename} gespeichert.")

        # --- 6. Eingangsdatei löschen nach erfolgreicher Verarbeitung ---
        try:
            input_bucket = storage_client.bucket(input_bucket_name)
            input_blob = input_bucket.blob(file_name)
            input_blob.delete()
            print(f"🗑️ Eingangsdatei gs://{input_bucket_name}/{file_name} erfolgreich gelöscht.")
        except Exception as delete_error:
            print(f"⚠️ Warnung: Eingangsdatei konnte nicht gelöscht werden: {delete_error}")
            # Fehler beim Löschen ist nicht kritisch, da das Dokument bereits verarbeitet wurde

    except Exception as e:
        print(f"❌ Fehler beim Speichern der Ergebnisdatei: {e}")
        # Eingangsdatei NICHT löschen, wenn die Verarbeitung fehlgeschlagen ist