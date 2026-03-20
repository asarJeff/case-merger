package com.asar.casemerge.web;

import com.asar.casemerge.service.CaseMergeService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AdminController {

  private final CaseMergeService svc;

  public AdminController(CaseMergeService svc) {
    this.svc = svc;
  }

  @PostMapping("/admin/run-once")
  public String runOnce() {
    svc.processOnce();
    return "OK";
  }
}
