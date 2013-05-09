/**
 * Copyright Maarten Billemont (http://www.lhunath.com, lhunath@lyndir.com)
 *
 * See the enclosed file LICENSE for license information (LGPLv3). If you did
 * not receive this file, see http://www.gnu.org/licenses/lgpl-3.0.txt
 *
 * @author   Maarten Billemont <lhunath@lyndir.com>
 * @license  http://www.gnu.org/licenses/lgpl-3.0.txt
 */

//
//  NSURL(USM).h
//  NSURL(USM)
//
//  Created by lhunath on 2013-05-08.
//  Copyright, lhunath (Maarten Billemont) 2013. All rights reserved.
//

#import "NSURL+UbiquityStoreManager.h"

@implementation NSURL(UbiquityStoreManager)

- (BOOL)downloadAndWait {

    do {
        CFURLClearResourcePropertyCache((__bridge CFURLRef)(self));
        
        NSNumber *ubiquitous, *conflicts, *downloaded, *downloading;
        if (![self getResourceValue:&ubiquitous forKey:NSURLIsUbiquitousItemKey error:nil])
            return NO;
        
        if (![ubiquitous boolValue])
            return YES;

        if (![self getResourceValue:&conflicts forKey:NSURLUbiquitousItemHasUnresolvedConflictsKey error:nil])
            return NO;
        
        if ([conflicts boolValue])
            return YES;
        
        if (![self getResourceValue:&downloaded forKey:NSURLUbiquitousItemIsDownloadedKey error:nil])
            return NO;

        if ([downloaded boolValue])
            return YES;

        if (![self getResourceValue:&downloading forKey:NSURLUbiquitousItemIsDownloadingKey error:nil])
            return NO;

        if (![downloading boolValue] && ![[NSFileManager defaultManager] startDownloadingUbiquitousItemAtURL:self error:nil])
            return NO;
        
        [NSThread sleepForTimeInterval:0.1];
    } while (true);
}

@end
