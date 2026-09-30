package io.translab.tantor.artifact.web;

import io.translab.tantor.artifact.service.AirGapBundleService;
import io.translab.tantor.artifact.util.RoleAuthenticationUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Tag(name = "Bundles", description = "Air-gapped export and import of artifact bundles")
@RestController
@RequestMapping("/api/v1/bundles")
public class BundleController {

    private final AirGapBundleService bundleService;
    private final RoleAuthenticationUtil roleAuthenticationUtil;

    public BundleController(AirGapBundleService bundleService,
                            RoleAuthenticationUtil roleAuthenticationUtil) {
        this.bundleService = bundleService;
        this.roleAuthenticationUtil = roleAuthenticationUtil;
    }

    @Operation(summary = "Export selected (or all AVAILABLE) artifacts as a .tar.gz bundle")
    @GetMapping(value = "/export", produces = "application/gzip")
    public ResponseEntity<StreamingResponseBody> export(
            @RequestParam(required = false) List<UUID> ids) {

        String stamp = OffsetDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        String fileName = "tantor-bundle-" + stamp + ".tar.gz";

        StreamingResponseBody body = out -> bundleService.export(ids, out);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                .contentType(MediaType.parseMediaType("application/gzip"))
                .body(body);
    }

    @Operation(summary = "Import a previously exported bundle (re-verifies every checksum)")
    @PostMapping(value = "/import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> importBundle(
            @RequestParam MultipartFile file,
            @RequestHeader(value = "Authorization", required = false) String authorization) throws IOException {
        if (!roleAuthenticationUtil.canAccess(authorization, RoleAuthenticationUtil.BUNDLE_IMPORT)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Unauthorized"));
        }
        int imported = bundleService.importBundle(file.getInputStream());
        return ResponseEntity.ok(Map.of("imported", imported, "fileName", file.getOriginalFilename()));
    }
}
