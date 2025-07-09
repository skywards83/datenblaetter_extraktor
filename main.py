# -*- coding: utf-8 -*-

import os
from google.cloud import documentai
from google.cloud import storage

from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=["POST"])
def http_entrypoint():
    # Cloud Functions (2nd gen) und Cloud Run schicken das Event als JSON im Body
    event = request.get_json(force=True)
    # Kontext ist optional, kann leer bleiben
    context = {}
    process_document_from_gcs(event, context)
    return ("", 204)

def process_document_from_gcs(event, context):
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

    # Sicherstellen, dass nur PDF-Dateien verarbeitet werden, um Fehler zu vermeiden.
    if not content_type == "application/pdf":
        print(f"⚠️ Datei '{file_name}' ist keine PDF-Datei ({content_type}). Verarbeitung wird übersprungen.")
        return

    # --- 3. Document AI und Storage Clients initialisieren ---
    # Es wird empfohlen, Clients innerhalb der Funktion zu initialisieren.
    storage_client = storage.Client()
    # WICHTIG: Den API-Endpunkt explizit basierend auf der Location setzen.
    # Dies stellt sicher, dass der Client mit der richtigen Google Cloud Region spricht.
    opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
    docai_client = documentai.DocumentProcessorServiceClient(client_options=opts)
 

    # Den vollständigen Pfad zum Document AI Prozessor zusammenbauen.
    processor_path = docai_client.processor_path(project_id, location, processor_id)

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

    except Exception as e:
        print(f"❌ Fehler beim Speichern der Ergebnisdatei: {e}")