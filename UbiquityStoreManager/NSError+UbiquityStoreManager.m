//
// Created by lhunath on 2013-03-13.
//
// To change the template use AppCode | Preferences | File Templates.
//


#import "NSError+UbiquityStoreManager.h"


NSString *const UbiquityManagedStoreDidDetectCorruptionNotification = @"UbiquityManagedStoreDidDetectCorruptionNotification";

@implementation NSError(UbiquityStoreManager)

- (id)init_USM_WithDomain:(NSString *)domain code:(NSInteger)code userInfo:(NSDictionary *)dict {

    self = [self init_USM_WithDomain:domain code:code userInfo:dict];
    if ([domain isEqualToString:NSCocoaErrorDomain] && code == 134302) {
        NSError *cause = [dict objectForKey:NSUnderlyingErrorKey];
        if (!cause)
            cause = [dict objectForKey:@"underlyingError"];

        if ([cause.domain isEqualToString:NSCocoaErrorDomain] && cause.code == 1570) {
            // Severity: Critical To Cloud Content
            // Cause: Validation Error -- The object being imported does not pass model validation: It is corrupt.
            // (it passed model validation on the other device just fine since it got saved!).
            // Action: Mark corrupt, request rebuild.
            [[NSNotificationCenter defaultCenter] postNotificationName:UbiquityManagedStoreDidDetectCorruptionNotification object:self];
        }
        else if ([(NSString *)[dict objectForKey:@"reason"] hasPrefix:@"Error reading the log file at location: (null)"]) {
            // Severity: Delayed Import?
            // Cause: Log file failed to download?
            // Action: Ignore.
        }
        else {
            NSLog( @"Detected unknown ubiquity import error." );
            NSLog( @"Please report this at http://lhunath.github.io/UbiquityStoreManager" );
            NSLog( @"and provide details of the conditions and whether or not you notice" );
            NSLog( @"any sync issues afterwards.  Error userInfo:" );
            for (id key in dict) {
                id value = [dict objectForKey:key];
                NSLog( @"[%@] %@ => [%@] %@", [key class], key, [value class], value );
            }
            NSLog( @"Error Debug Description:\n%@", [self debugDescription] );
        }
    }

    return self;
}

@end
